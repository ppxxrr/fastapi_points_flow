/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}"
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          sky: "#38BDF8",
          blue: "#2563EB",
          indigo: "#4F46E5",
          violet: "#7C3AED"
        }
      },
      boxShadow: {
        glass: "0 24px 80px rgba(71, 93, 160, 0.16)",
        panel: "0 18px 42px rgba(15, 23, 42, 0.06)"
      },
      borderRadius: {
        "4xl": "2rem"
      },
      fontFamily: {
        code: ["Fira Code", "monospace"],
        sans: ["Fira Sans", "ui-sans-serif", "system-ui", "sans-serif"]
      }
    }
  },
  plugins: []
};
