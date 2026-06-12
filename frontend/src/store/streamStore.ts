import { create } from 'zustand'
import type { StreamMetricSnapshot } from '../types/stream.types'

interface StreamStoreState {
  /** Map of streamId → latest StreamMetricSnapshot */
  streams: Record<string, StreamMetricSnapshot>
  /** Upsert a snapshot into the store, keyed by snapshot.streamId */
  setSnapshot: (snapshot: StreamMetricSnapshot) => void
}

export const useStreamStore = create<StreamStoreState>((set) => ({
  streams: {},
  setSnapshot: (snapshot) =>
    set((state) => ({
      streams: {
        ...state.streams,
        [snapshot.streamId]: snapshot,
      },
    })),
}))
