import { defineMDSveXConfig as defineConfig } from "mdsvex";

const config = defineConfig({
  extensions: [".svelte.md", ".md", ".svx"],

  smartypants: {
    dashes: "oldschool",
  },

  remarkPlugins: [],
  rehypePlugins: [],

  highlight: {
    highlighter: async (code, lang = "text") => {
      const { getHighlighter } = await import("shiki");
      const highlighter = await getHighlighter({
        theme: "github-dark",
      });
      const html = highlighter.codeToHtml(code, { lang });
      return `{@html \`${html}\`}`;
    },
  },

  layout: {
    _: "./src/lib/components/markdown-layout.svelte",
  },
});

export default config;
