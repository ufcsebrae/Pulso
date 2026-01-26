// tailwind.config.js
/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./templates/**/*.html"],
  theme: {
    extend: {
      colors: {
        'brand-primary': '#4F46E5',         // Roxo principal para destaques
        'project-exclusive': '#3B82F6',    // Azul para projetos exclusivos
        'project-shared': '#10B981',       // Verde para projetos compartilhados
        'alert-danger': '#EF4444',         // Vermelho para alertas (Inércia, gastos não planejados)
        'alert-warning': '#D97706',        // Laranja para avisos
      },
    },
  },
  plugins: [],
}
