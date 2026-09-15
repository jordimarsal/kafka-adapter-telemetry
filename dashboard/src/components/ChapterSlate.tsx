import { AnimatePresence, motion } from 'framer-motion'
import { useDashboard } from '../stream/store'

const SUBTITLE: Record<string, string> = {
  low: '20 events · 1 ev/s',
  moderate: '100 events · 10 ev/s',
  high: '500 events · 50 ev/s · alerts armed',
  overload: '2000 events · 200 ev/s · dupes ON · corrupt ON',
}

export function ChapterSlate() {
  const phase = useDashboard(state => state.phase)
  return (
    <AnimatePresence>
      {phase && (
        <motion.div
          key={`${phase.index}-${phase.name}`}
          initial={{ y: -48, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: -48, opacity: 0 }}
          transition={{ duration: 0.3 }}
          className="fixed inset-x-0 top-0 z-10 bg-up/10 py-2 text-center text-xs tracking-[0.3em] text-up"
        >
          PHASE {phase.index}/{phase.total} — {phase.name.toUpperCase()} · {SUBTITLE[phase.name] ?? ''}
        </motion.div>
      )}
    </AnimatePresence>
  )
}
