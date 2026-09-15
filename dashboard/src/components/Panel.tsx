import type { ReactNode } from 'react'

export function Panel({ label, children }: { label: string; children: ReactNode }) {
  return (
    <section className="flex min-h-0 flex-col rounded-lg border border-line bg-panel p-3">
      <h2 className="cap mb-2">{label}</h2>
      <div className="min-h-0 flex-1">{children}</div>
    </section>
  )
}
