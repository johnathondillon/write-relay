export default defineNuxtConfig({
  buildDir: '.nuxt',
  compatibilityDate: "2026-09-08",
  devtools: { enabled: false },
  telemetry: false,
  nitro: { preset: "node-server" },
  app: {
    head: { title: "WriteRelay · Learning lab", htmlAttrs: { lang: "en" } },
  },
});
