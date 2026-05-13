#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InlineKind {
    Code,
    Image,
    Strike,
    Bold3,
    Italic,
    UBold3,
    UItalic,
    Table,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InlineState {
    kind: InlineKind,
    acc: String,
}

#[derive(Default)]
pub(crate) struct StreamingMarkdownFilter {
    buf: String,
    fence: bool,
    sol: bool,
    inline: Option<InlineState>,
}

impl StreamingMarkdownFilter {
    pub(crate) fn new() -> Self {
        Self {
            sol: true,
            ..Self::default()
        }
    }

    pub(crate) fn feed(&mut self, delta: &str) -> String {
        self.buf.push_str(delta);
        self.pump(false)
    }

    pub(crate) fn flush(&mut self) -> String {
        self.pump(true)
    }

    fn pump(&mut self, eof: bool) -> String {
        let mut out = String::new();
        while !self.buf.is_empty() {
            let before_len = self.buf.len();
            let before_sol = self.sol;
            let before_fence = self.fence;
            let before_inline = self.inline.clone();

            if self.fence {
                out.push_str(&self.pump_fence(eof));
            } else if self.inline.is_some() {
                out.push_str(&self.pump_inline());
            } else if self.sol {
                out.push_str(&self.pump_sol(eof));
            } else {
                out.push_str(&self.pump_body(eof));
            }

            if self.buf.len() == before_len
                && self.sol == before_sol
                && self.fence == before_fence
                && self.inline == before_inline
            {
                break;
            }
        }

        if eof {
            if let Some(inline) = self.inline.take() {
                if inline.kind == InlineKind::Table {
                    out.push_str(&extract_table_row(&inline.acc));
                } else {
                    out.push_str(inline_marker(inline.kind));
                    out.push_str(&inline.acc);
                }
            }
        }
        out
    }

    fn pump_fence(&mut self, eof: bool) -> String {
        if self.sol {
            if self.buf.len() < 3 && !eof {
                return String::new();
            }
            if self.buf.starts_with("```") {
                self.fence = false;
                self.buf = match self.buf[3..].find('\n') {
                    Some(offset) => self.buf[3 + offset + 1..].to_string(),
                    None => String::new(),
                };
                self.sol = true;
                return String::new();
            }
            self.sol = false;
        }

        if let Some(nl) = self.buf.find('\n') {
            let chunk = self.buf[..=nl].to_string();
            self.buf = self.buf[nl + 1..].to_string();
            self.sol = true;
            return chunk;
        }

        std::mem::take(&mut self.buf)
    }

    fn pump_sol(&mut self, eof: bool) -> String {
        if self.buf.starts_with('\n') {
            self.buf.remove(0);
            return "\n".to_string();
        }

        let bytes = self.buf.as_bytes();
        match bytes.first().copied() {
            Some(b'`') => {
                if self.buf.len() < 3 && !eof {
                    return String::new();
                }
                if self.buf.starts_with("```") {
                    self.fence = true;
                    self.buf = match self.buf[3..].find('\n') {
                        Some(offset) => self.buf[3 + offset + 1..].to_string(),
                        None => String::new(),
                    };
                    self.sol = true;
                    return String::new();
                }
                self.sol = false;
                String::new()
            }
            Some(b'>') => {
                if self.buf.len() < 2 && !eof {
                    return String::new();
                }
                self.buf = if self.buf.as_bytes().get(1) == Some(&b' ') {
                    self.buf[2..].to_string()
                } else {
                    self.buf[1..].to_string()
                };
                self.sol = false;
                String::new()
            }
            Some(b'#') => {
                let mut n = 0;
                while self.buf.as_bytes().get(n) == Some(&b'#') {
                    n += 1;
                }
                if n == self.buf.len() && !eof {
                    return String::new();
                }
                if (5..=6).contains(&n) && self.buf.as_bytes().get(n) == Some(&b' ') {
                    self.buf = self.buf[n + 1..].to_string();
                    self.sol = false;
                    return String::new();
                }
                self.sol = false;
                String::new()
            }
            Some(b'|') => {
                self.buf = self.buf[1..].to_string();
                self.inline = Some(InlineState {
                    kind: InlineKind::Table,
                    acc: String::new(),
                });
                self.sol = false;
                String::new()
            }
            Some(b' ' | b'\t') => {
                if !eof && self.buf.bytes().all(|b| b == b' ' || b == b'\t') {
                    return String::new();
                }
                self.sol = false;
                String::new()
            }
            Some(b'-' | b'*' | b'_') => {
                let ch = bytes[0];
                let mut j = 0;
                while matches!(self.buf.as_bytes().get(j), Some(&b) if b == ch || b == b' ') {
                    j += 1;
                }
                if j == self.buf.len() && !eof {
                    return String::new();
                }
                if j == self.buf.len() || self.buf.as_bytes().get(j) == Some(&b'\n') {
                    let count = self.buf.as_bytes()[..j]
                        .iter()
                        .filter(|&&b| b == ch)
                        .count();
                    if count >= 3 {
                        self.buf = if j < self.buf.len() {
                            self.buf[j + 1..].to_string()
                        } else {
                            String::new()
                        };
                        self.sol = true;
                        return String::new();
                    }
                }
                self.sol = false;
                String::new()
            }
            _ => {
                self.sol = false;
                String::new()
            }
        }
    }

    fn pump_body(&mut self, eof: bool) -> String {
        let mut i = 0;
        while i < self.buf.len() {
            let c = self.buf.as_bytes()[i];
            match c {
                b'\n' => {
                    let out = self.buf[..=i].to_string();
                    self.buf = self.buf[i + 1..].to_string();
                    self.sol = true;
                    return out;
                }
                b'`' => return self.start_inline(i, 1, InlineKind::Code),
                b'!' if self.buf.as_bytes().get(i + 1) == Some(&b'[') => {
                    return self.start_inline(i, 2, InlineKind::Image);
                }
                b'~' if self.buf.as_bytes().get(i + 1) == Some(&b'~') => {
                    return self.start_inline(i, 2, InlineKind::Strike);
                }
                b'*' => {
                    if self.buf.as_bytes().get(i + 1) == Some(&b'*')
                        && self.buf.as_bytes().get(i + 2) == Some(&b'*')
                    {
                        return self.start_inline(i, 3, InlineKind::Bold3);
                    }
                    if self.buf.as_bytes().get(i + 1) == Some(&b'*') {
                        i += 2;
                        continue;
                    }
                    if matches!(self.buf.as_bytes().get(i + 1), Some(&b) if b != b' ' && b != b'\n')
                    {
                        return self.start_inline(i, 1, InlineKind::Italic);
                    }
                    i += 1;
                    continue;
                }
                b'_' => {
                    if self.buf.as_bytes().get(i + 1) == Some(&b'_')
                        && self.buf.as_bytes().get(i + 2) == Some(&b'_')
                    {
                        return self.start_inline(i, 3, InlineKind::UBold3);
                    }
                    if self.buf.as_bytes().get(i + 1) == Some(&b'_') {
                        i += 2;
                        continue;
                    }
                    if matches!(self.buf.as_bytes().get(i + 1), Some(&b) if b != b' ' && b != b'\n')
                    {
                        return self.start_inline(i, 1, InlineKind::UItalic);
                    }
                    i += 1;
                    continue;
                }
                _ => i += 1,
            }
        }

        let hold = if !eof {
            if self.buf.ends_with("**") || self.buf.ends_with("__") {
                2
            } else if self.buf.ends_with('*')
                || self.buf.ends_with('_')
                || self.buf.ends_with('~')
                || self.buf.ends_with('!')
            {
                1
            } else {
                0
            }
        } else {
            0
        };
        let split = self.buf.len() - hold;
        let out = self.buf[..split].to_string();
        self.buf = if hold > 0 {
            self.buf[split..].to_string()
        } else {
            String::new()
        };
        out
    }

    fn start_inline(&mut self, marker_at: usize, marker_len: usize, kind: InlineKind) -> String {
        let out = self.buf[..marker_at].to_string();
        self.buf = self.buf[marker_at + marker_len..].to_string();
        self.inline = Some(InlineState {
            kind,
            acc: String::new(),
        });
        out
    }

    fn pump_inline(&mut self) -> String {
        let Some(mut inline) = self.inline.take() else {
            return String::new();
        };
        inline.acc.push_str(&self.buf);
        self.buf.clear();

        let out = match inline.kind {
            InlineKind::Code => self.close_simple_inline(&inline, "`", 1).or_else(|| {
                inline.acc.find('\n').map(|nl| {
                    let out = format!("`{}", &inline.acc[..=nl]);
                    self.buf = inline.acc[nl + 1..].to_string();
                    self.sol = true;
                    out
                })
            }),
            InlineKind::Strike => self.close_simple_inline(&inline, "~~", 2),
            InlineKind::Bold3 => self.close_simple_inline(&inline, "***", 3),
            InlineKind::UBold3 => self.close_simple_inline(&inline, "___", 3),
            InlineKind::Italic => self.close_em_inline(&inline, b'*'),
            InlineKind::UItalic => self.close_em_inline(&inline, b'_'),
            InlineKind::Image => self.close_image_inline(&inline),
            InlineKind::Table => inline.acc.find('\n').map(|nl| {
                let line = &inline.acc[..nl];
                self.buf = inline.acc[nl + 1..].to_string();
                self.sol = true;
                let row = extract_table_row(line);
                if row.is_empty() {
                    String::new()
                } else {
                    format!("{row}\n")
                }
            }),
        };

        if let Some(out) = out {
            out
        } else {
            self.inline = Some(inline);
            String::new()
        }
    }

    fn close_simple_inline(
        &mut self,
        inline: &InlineState,
        marker: &str,
        marker_len: usize,
    ) -> Option<String> {
        inline.acc.find(marker).map(|idx| {
            let out = inline.acc[..idx].to_string();
            self.buf = inline.acc[idx + marker_len..].to_string();
            out
        })
    }

    fn close_em_inline(&mut self, inline: &InlineState, marker: u8) -> Option<String> {
        let bytes = inline.acc.as_bytes();
        let mut j = 0;
        while j < bytes.len() {
            if bytes[j] == b'\n' {
                let out = format!("{}{}", marker as char, &inline.acc[..=j]);
                self.buf = inline.acc[j + 1..].to_string();
                self.sol = true;
                return Some(out);
            }
            if bytes[j] == marker {
                if bytes.get(j + 1) == Some(&marker) {
                    j += 2;
                    continue;
                }
                let out = inline.acc[..j].to_string();
                self.buf = inline.acc[j + 1..].to_string();
                return Some(out);
            }
            j += 1;
        }
        None
    }

    fn close_image_inline(&mut self, inline: &InlineState) -> Option<String> {
        let cb = inline.acc.find(']')?;
        if cb + 1 >= inline.acc.len() {
            return None;
        }
        if inline.acc.as_bytes().get(cb + 1) != Some(&b'(') {
            let out = format!("![{}", &inline.acc[..=cb]);
            self.buf = inline.acc[cb + 1..].to_string();
            return Some(out);
        }
        inline.acc[cb + 2..].find(')').map(|offset| {
            let cp = cb + 2 + offset;
            self.buf = inline.acc[cp + 1..].to_string();
            String::new()
        })
    }
}

pub fn filter_markdown(text: &str) -> String {
    let mut filter = StreamingMarkdownFilter::new();
    let mut out = filter.feed(text);
    out.push_str(&filter.flush());
    out
}

fn inline_marker(kind: InlineKind) -> &'static str {
    match kind {
        InlineKind::Code => "`",
        InlineKind::Image => "![",
        InlineKind::Strike => "~~",
        InlineKind::Bold3 => "***",
        InlineKind::Italic => "*",
        InlineKind::UBold3 => "___",
        InlineKind::UItalic => "_",
        InlineKind::Table => "",
    }
}

fn extract_table_row(line: &str) -> String {
    if line.contains('-')
        && line
            .chars()
            .all(|ch| ch.is_ascii_whitespace() || matches!(ch, '|' | ':' | '-'))
    {
        return String::new();
    }

    let parts = line.split('|').map(str::trim).collect::<Vec<_>>();
    let start = usize::from(parts.first() == Some(&""));
    let end = if parts.last() == Some(&"") {
        parts.len().saturating_sub(1)
    } else {
        parts.len()
    };
    parts[start..end].join("\t")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_shot(input: &str) -> String {
        filter_markdown(input)
    }

    fn char_by_char(input: &str) -> String {
        let mut filter = StreamingMarkdownFilter::new();
        let mut out = String::new();
        for ch in input.chars() {
            out.push_str(&filter.feed(&ch.to_string()));
        }
        out.push_str(&filter.flush());
        out
    }

    fn random_chunks(input: &str, seed: u32) -> String {
        let mut filter = StreamingMarkdownFilter::new();
        let mut out = String::new();
        let mut pos = 0;
        let mut s = seed;
        while pos < input.len() {
            s = s.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x7fff_ffff;
            let mut end = (pos + (s as usize % 5) + 1).min(input.len());
            while end < input.len() && !input.is_char_boundary(end) {
                end += 1;
            }
            out.push_str(&filter.feed(&input[pos..end]));
            pos = end;
        }
        out.push_str(&filter.flush());
        out
    }

    fn expect_filter(input: &str, expected: &str) {
        assert_eq!(one_shot(input), expected, "one-shot mismatch");
        assert_eq!(char_by_char(input), expected, "char stream mismatch");
        assert_eq!(
            random_chunks(input, 42),
            expected,
            "random chunk stream mismatch"
        );
    }

    #[test]
    fn plain_text_passthrough_matches_typescript_sdk() {
        expect_filter("hello world", "hello world");
        expect_filter("", "");
        expect_filter("line1\nline2\nline3", "line1\nline2\nline3");
        expect_filter("cafe monde", "cafe monde");
        expect_filter("Hello cafe World", "Hello cafe World");
        expect_filter("Grüße café 🌟", "Grüße café 🌟");
        expect_filter("Hello naïve World 🌍", "Hello naïve World 🌍");
    }

    #[test]
    fn code_fences_match_typescript_sdk() {
        assert_eq!(one_shot("```\ncode\n```\n"), "code\n");
        assert_eq!(
            one_shot("```typescript\nconst x = 1;\n```\n"),
            "const x = 1;\n"
        );
        assert_eq!(
            one_shot("before\n```\ncode\n```\nafter"),
            "before\ncode\nafter"
        );
        assert_eq!(
            one_shot("```\n**bold** *italic* ~~strike~~\n```\n"),
            "**bold** *italic* ~~strike~~\n"
        );
        assert_eq!(
            one_shot("```\nblock1\n```\ntext\n```\nblock2\n```\n"),
            "block1\ntext\nblock2\n"
        );
        assert_eq!(one_shot("```\ncode\n```"), "code\n");

        let mut split_marker = StreamingMarkdownFilter::new();
        let out =
            split_marker.feed("```") + &split_marker.feed("\ncode\n```\n") + &split_marker.flush();
        assert_eq!(out, "\ncode\n");

        let mut split_line = StreamingMarkdownFilter::new();
        let out = split_line.feed("```\n") + &split_line.feed("code\n") + &split_line.feed("```\n");
        assert_eq!(out + &split_line.flush(), "code\n");

        let mut language = StreamingMarkdownFilter::new();
        let out = language.feed("```typescript\n")
            + &language.feed("const x = 1;\n")
            + &language.feed("```\n")
            + &language.flush();
        assert_eq!(out, "const x = 1;\n");
    }

    #[test]
    fn inline_code_matches_typescript_sdk() {
        expect_filter("use `fmt.Println` here", "use fmt.Println here");
        expect_filter("text\n`code`", "text\ncode");
        expect_filter("hello `world\nnext", "hello `world\nnext");
        expect_filter("run `rm -rf /` carefully", "run rm -rf / carefully");
    }

    #[test]
    fn images_match_typescript_sdk() {
        expect_filter("![alt](http://example.com/img.png)", "");
        expect_filter("before ![alt](url) after", "before  after");
        expect_filter("![not an image] text", "![not an image] text");
        expect_filter("![a](u1)![b](u2)", "");

        let mut filter = StreamingMarkdownFilter::new();
        let result = filter.feed("![alt](url") + &filter.flush();
        assert_eq!(result, "![alt](url");
    }

    #[test]
    fn strikethrough_matches_typescript_sdk() {
        expect_filter("~~deleted~~", "deleted");
        expect_filter("keep ~~this~~ too", "keep this too");

        let mut filter = StreamingMarkdownFilter::new();
        let result = filter.feed("~~unclosed") + &filter.flush();
        assert_eq!(result, "~~unclosed");
    }

    #[test]
    fn double_star_bold_is_preserved_like_typescript_sdk() {
        expect_filter("**bold**", "**bold**");
        expect_filter("this is **very** important", "this is **very** important");
        expect_filter("**a** and **b**", "**a** and **b**");
    }

    #[test]
    fn single_star_italic_matches_typescript_sdk() {
        expect_filter("*italic*", "italic");
        expect_filter("this is *emphasized* text", "this is emphasized text");
        expect_filter("*unclosed\nnext", "*unclosed\nnext");
        expect_filter("3 * 4 = 12", "3 * 4 = 12");
        expect_filter("3 *\nnext", "3 *\nnext");
    }

    #[test]
    fn triple_star_bold_italic_matches_typescript_sdk() {
        expect_filter("***bold italic***", "bold italic");
        expect_filter("this is ***very strong*** text", "this is very strong text");

        let mut filter = StreamingMarkdownFilter::new();
        let result = filter.feed("***unclosed") + &filter.flush();
        assert_eq!(result, "***unclosed");
    }

    #[test]
    fn blockquotes_match_typescript_sdk() {
        expect_filter("> quoted text", "quoted text");
        expect_filter(">quoted", "quoted");
        expect_filter("> line1\n> line2", "line1\nline2");
        expect_filter("> **bold** in quote", "**bold** in quote");
    }

    #[test]
    fn headings_match_typescript_sdk() {
        expect_filter("# Title", "# Title");
        expect_filter("## Subtitle", "## Subtitle");
        expect_filter("### Section", "### Section");
        expect_filter("#### Subsection", "#### Subsection");
        expect_filter("##### Small Heading", "Small Heading");
        expect_filter("###### Tiny Heading", "Tiny Heading");
        expect_filter("## Title\nbody text", "## Title\nbody text");
        expect_filter("##### Title\nbody text", "Title\nbody text");
    }

    #[test]
    fn horizontal_rules_match_typescript_sdk() {
        expect_filter("before\n---\nafter", "before\nafter");
        expect_filter("before\n***\nafter", "before\nafter");
        expect_filter("before\n___\nafter", "before\nafter");
        expect_filter("before\n- - -\nafter", "before\nafter");
        expect_filter("text\n---", "text\n");
        expect_filter("text\n--\nnext", "text\n--\nnext");
    }

    #[test]
    fn tables_match_typescript_sdk() {
        let table = "| Header1 | Header2 |\n|---------|---------||\n| Cell1 | Cell2 |";
        let result = one_shot(table);
        assert!(!result.contains('|'));
        assert!(result.contains("Header1"));
        assert!(result.contains("Header2"));
        assert!(result.contains("Cell1"));
        assert!(result.contains("Cell2"));

        let separator = one_shot("| A | B |\n|---|---|\n| 1 | 2 |");
        assert!(!separator.contains("---"));
        assert!(!separator.contains('|'));
        assert!(separator.contains('A'));
        assert!(separator.contains('B'));
        assert!(separator.contains('1'));
        assert!(separator.contains('2'));

        assert_eq!(one_shot("| A | B |\n"), "A\tB\n");
        assert_eq!(one_shot("|:---|---:|\n"), "");

        let surrounding = "Results:\n| A | B |\n|---|---|\n| 1 | 2 |\nDone.";
        let result = one_shot(surrounding);
        assert!(result.contains("Results:"));
        assert!(result.contains("Done."));
        assert!(!result.contains('|'));
        assert!(!result.contains("---"));
        assert!(result.contains('A'));
        assert!(result.contains('2'));

        let emoji_table = [
            "| Token | Emoji |",
            "|-------|-------|",
            "| smile | 🙂 |",
            "| face | 😐 |",
        ]
        .join("\n");
        let result = one_shot(&emoji_table);
        assert!(!result.contains('|'));
        assert!(!result.contains("---"));
        assert!(result.contains("Token"));
        assert!(result.contains('🙂'));
        assert!(result.contains("smile"));

        assert_eq!(one_shot("| A | B |"), "A\tB");

        let mut row_split = StreamingMarkdownFilter::new();
        let out = row_split.feed("| A |") + &row_split.feed(" B |\n") + &row_split.flush();
        assert_eq!(out, "A\tB\n");

        let mut separator_split = StreamingMarkdownFilter::new();
        let out = separator_split.feed("|---")
            + &separator_split.feed("|---|\n")
            + &separator_split.flush();
        assert_eq!(out, "");

        assert_eq!(one_shot("| just text\n"), "just text\n");
    }

    #[test]
    fn lists_match_typescript_sdk() {
        expect_filter("- item 1\n- item 2", "- item 1\n- item 2");
        expect_filter("* item 1\n* item 2", "* item 1\n* item 2");
        assert_eq!(one_shot("  - nested item"), "  - nested item");
        assert_eq!(one_shot("      - deep item"), "      - deep item");
        assert_eq!(one_shot("  * nested"), "  * nested");
        assert_eq!(
            one_shot("- top\n  - nested\n- top2"),
            "- top\n  - nested\n- top2"
        );

        let mut filter = StreamingMarkdownFilter::new();
        let out = filter.feed("  - nested item") + &filter.flush();
        assert_eq!(out, "  - nested item");

        let mut split = StreamingMarkdownFilter::new();
        let out = split.feed("  ") + &split.feed("- nested") + &split.flush();
        assert_eq!(out, "  - nested");
    }

    #[test]
    fn combined_patterns_match_typescript_sdk() {
        expect_filter(
            "## **Title**\nUse `code` here.",
            "## **Title**\nUse code here.",
        );
        expect_filter("> *italic* and ~~strike~~", "italic and strike");
        assert_eq!(
            one_shot("```\nfenced\n```\n`inline` ![img](url)"),
            "fenced\ninline "
        );
        expect_filter(
            "**bold** then ***bold-italic*** then **bold2**",
            "**bold** then bold-italic then **bold2**",
        );

        let input = [
            "## Summary",
            "",
            "> This is a quote.",
            "",
            "Here is **important** and *emphasized* text.",
            "",
            "```python",
            "print('hello')",
            "```",
            "",
            "- item 1",
            "  - nested",
            "- item 2",
            "",
            "---",
            "",
            "End.",
        ]
        .join("\n");

        let result = one_shot(&input);
        assert!(result.contains("## Summary"));
        assert!(result.contains("**important**"));
        assert!(result.contains("emphasized"));
        assert!(!result.contains("*emphasized*"));
        assert!(result.contains("print('hello')"));
        assert!(!result.contains("```"));
        assert!(result.contains("- item 1"));
        assert!(result.contains("- nested"));
        assert!(!result.contains("---"));
        assert!(result.contains("End."));
    }

    #[test]
    fn hold_back_logic_matches_typescript_sdk() {
        let mut filter = StreamingMarkdownFilter::new();
        assert_eq!(filter.feed("hello *"), "hello ");
        assert_eq!(filter.feed("world* end"), "world end");
        assert_eq!(filter.flush(), "");

        let mut space = StreamingMarkdownFilter::new();
        assert_eq!(space.feed("3 *"), "3 ");
        assert_eq!(space.feed(" 4"), "* 4");
        assert_eq!(space.flush(), "");

        let mut double = StreamingMarkdownFilter::new();
        assert_eq!(double.feed("text **"), "text ");
        assert_eq!(double.feed("bold** end"), "**bold** end");
        assert_eq!(double.flush(), "");

        let mut triple = StreamingMarkdownFilter::new();
        assert_eq!(triple.feed("a **"), "a ");
        assert_eq!(triple.feed("*bi*** end"), "bi end");
        assert_eq!(triple.flush(), "");

        let mut strike = StreamingMarkdownFilter::new();
        assert_eq!(strike.feed("text ~"), "text ");
        assert_eq!(strike.feed("~strike~~ end"), "strike end");
        assert_eq!(strike.flush(), "");

        let mut image = StreamingMarkdownFilter::new();
        assert_eq!(image.feed("see !"), "see ");
        assert_eq!(image.feed("[alt](url) end"), " end");
        assert_eq!(image.flush(), "");

        let mut bang = StreamingMarkdownFilter::new();
        assert_eq!(bang.feed("wow!"), "wow");
        assert_eq!(bang.feed(" great"), "! great");
        assert_eq!(bang.flush(), "");
    }

    #[test]
    fn eof_and_flush_behavior_matches_typescript_sdk() {
        let mut held = StreamingMarkdownFilter::new();
        held.feed("trailing *");
        assert_eq!(held.flush(), "*");

        let mut filter = StreamingMarkdownFilter::new();
        let result = filter.feed("unclosed `code") + &filter.flush();
        assert_eq!(result, "unclosed `code");

        let mut strike = StreamingMarkdownFilter::new();
        let result = strike.feed("~~unclosed") + &strike.flush();
        assert_eq!(result, "~~unclosed");

        let mut bold_italic = StreamingMarkdownFilter::new();
        let result = bold_italic.feed("***unclosed") + &bold_italic.flush();
        assert_eq!(result, "***unclosed");

        let mut italic = StreamingMarkdownFilter::new();
        let result = italic.feed("*unclosed") + &italic.flush();
        assert_eq!(result, "*unclosed");

        let mut image = StreamingMarkdownFilter::new();
        let result = image.feed("![alt text") + &image.flush();
        assert_eq!(result, "![alt text");

        let mut idempotent = StreamingMarkdownFilter::new();
        let feed_out = idempotent.feed("hello **bold**");
        let r1 = idempotent.flush();
        let r2 = idempotent.flush();
        assert_eq!(feed_out + &r1 + &r2, "hello **bold**");
        assert_eq!(r2, "");
    }

    #[test]
    fn streaming_consistency_matches_typescript_sdk() {
        let cases = [
            ("plain text", "plain text"),
            ("**bold** text", "**bold** text"),
            ("*italic* text", "italic text"),
            ("***bi*** text", "bi text"),
            ("~~strike~~ text", "strike text"),
            ("`code` text", "code text"),
            ("![img](url)", ""),
            ("> blockquote", "blockquote"),
            ("##### H5 heading", "H5 heading"),
            ("## H2 heading", "## H2 heading"),
            ("before\n---\nafter", "before\nafter"),
            (
                "Here **bold** and *italic* `code` ~~strike~~ ***bi*** end",
                "Here **bold** and italic code strike bi end",
            ),
        ];

        for (input, expected) in cases {
            expect_filter(input, expected);
        }

        let input = "```\nfenced\n```\nafter";
        assert_eq!(one_shot(input), "fenced\nafter");
        let mut fence = StreamingMarkdownFilter::new();
        let out = fence.feed("```\n")
            + &fence.feed("fenced\n")
            + &fence.feed("```\n")
            + &fence.feed("after")
            + &fence.flush();
        assert_eq!(out, "fenced\nafter");

        let input = "  - nested";
        assert_eq!(one_shot(input), "  - nested");
        let mut list = StreamingMarkdownFilter::new();
        let out = list.feed("  - nested") + &list.flush();
        assert_eq!(out, "  - nested");
    }

    #[test]
    fn edge_cases_match_typescript_sdk() {
        let result = one_shot("**b***i*");
        assert!(result.contains("**b**"));
        assert_eq!(char_by_char("**b***i*"), one_shot("**b***i*"));

        expect_filter("* item\n* item2", "* item\n* item2");
        expect_filter("- item\n- item2", "- item\n- item2");
        expect_filter("   \n  \n", "   \n  \n");
        expect_filter("\n\n\n", "\n\n\n");
        expect_filter(">> deeply nested", "> deeply nested");
        expect_filter("see ![a](u1) and ![b](u2) end", "see  and  end");
        assert_eq!(one_shot("```\n**not bold**\n```\n"), "**not bold**\n");

        let long_text = "word ".repeat(1000);
        expect_filter(&long_text, &long_text);

        expect_filter("*a* **b** *c* **d**", "a **b** c **d**");
        expect_filter("- - -\n", "");
        expect_filter("- item", "- item");
    }
}
