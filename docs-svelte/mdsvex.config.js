import { defineMDSveXConfig as defineConfig } from "mdsvex";

const config = defineConfig({
  extensions: [".md"],

  smartypants: {
    dashes: "oldschool",
  },

  remarkPlugins: [],
  rehypePlugins: [],

  // Disable built-in highlighting, we'll use CSS
  highlight: false,

  layout: "$lib/components/markdown-layout.svelte"
});

export default config;