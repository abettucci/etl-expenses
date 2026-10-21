module.exports = {
  plugins: [
    require('tailwindcss')({ config: './tailwind.ticket-splitter.cjs' }),
    {
      postcssPlugin: 'scope-ticket-splitter-defaults',
      OnceExit(root) {
        if (!root.source?.input?.file?.endsWith('classic.css')) return
        root.walkRules(rule => {
          const selectors = rule.selector.split(',').map(selector => selector.trim())
          if (selectors.every(selector => ['*', '::before', '::after', '::backdrop'].includes(selector))) {
            rule.selector = selectors.flatMap(selector => selector === '*'
              ? ['#ticket-splitter', '#ticket-splitter *']
              : [`#ticket-splitter${selector}`, `#ticket-splitter ${selector}`]).join(', ')
          }
        })
      }
    }
  ]
}
