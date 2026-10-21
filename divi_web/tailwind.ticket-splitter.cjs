/** Original Ticket Splitter utilities, scoped to its own surface. */
module.exports = {
  content: ['./src/ticket_splitter/**/*.{ts,tsx}'],
  important: '#ticket-splitter',
  corePlugins: { preflight: false, container: false },
  theme: {
    container: { center: true, padding: '2rem', screens: { '2xl': '1400px' } },
    extend: {}
  },
  plugins: []
}
