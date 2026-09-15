import { AnimatePresence, motion } from 'framer-motion'
import { useDashboard } from '../stream/store'
import { Panel } from './Panel'

export function AlertsFeed() {
  const feed = useDashboard(state => state.alertsFeed)
  return (
    <Panel label={`alerts · ${feed.length}`}>
      <ul className="flex flex-col gap-1 overflow-hidden text-[10px]">
        <AnimatePresence initial={false}>
          {feed.slice(0, 12).map(item => (
            <motion.li
              key={item.seq}
              layout
              initial={{ opacity: 0, x: 12 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0 }}
              className="rounded border border-down/40 bg-panel px-2 py-1"
            >
              <span className="text-down">▲ {item.adapterId}</span>{' '}
              <span className="text-muted">{item.reason}</span>
            </motion.li>
          ))}
        </AnimatePresence>
      </ul>
    </Panel>
  )
}
