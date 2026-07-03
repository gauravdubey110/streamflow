/**
 * ChaosButton component tests.
 *
 * Spec ref: SPEC-16 R5 — POST to API on inject, button disabled during chaos,
 * cancel calls DELETE endpoint, toast on success/error.
 */
import { describe, it, expect, vi, afterEach, beforeAll } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import { ChaosButton } from '../components/controls/ChaosButton'

// Mock react-hot-toast so we can assert toast calls without rendering the portal.
vi.mock('react-hot-toast', () => ({
  default: {
    success: vi.fn(),
    error: vi.fn(),
  },
}))

// Mock the api service module.
vi.mock('../services/api', () => ({
  injectChaos: vi.fn(),
  cancelChaos: vi.fn(),
}))

import toast from 'react-hot-toast'
import { injectChaos, cancelChaos } from '../services/api'

const mockInjectChaos = vi.mocked(injectChaos)
const mockCancelChaos = vi.mocked(cancelChaos)
const mockToastSuccess = vi.mocked(toast.success)
const mockToastError = vi.mocked(toast.error)

// Polyfill ResizeObserver if needed (Recharts dep).
beforeAll(() => {
  if (!globalThis.ResizeObserver) {
    class MockResizeObserver {
      observe = vi.fn()
      unobserve = vi.fn()
      disconnect = vi.fn()
    }
    globalThis.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver
  }
})

afterEach(() => {
  vi.clearAllMocks()
})

describe('ChaosButton', () => {
  it('renders Inject Chaos button by default', () => {
    render(<ChaosButton streamId="stream-001" />)
    expect(screen.getByTestId('chaos-inject-btn')).toBeDefined()
    expect(screen.getByText('Inject Chaos')).toBeDefined()
  })

  it('renders scenario and duration selects', () => {
    render(<ChaosButton streamId="stream-001" />)
    expect(screen.getByTestId('chaos-scenario-select')).toBeDefined()
    expect(screen.getByTestId('chaos-duration-select')).toBeDefined()
  })

  it('calls injectChaos and shows success toast on click', async () => {
    mockInjectChaos.mockResolvedValueOnce({ chaosId: 'chaos-123', startsAt: Date.now() })

    render(<ChaosButton streamId="stream-001" />)

    await act(async () => {
      fireEvent.click(screen.getByTestId('chaos-inject-btn'))
    })

    await waitFor(() => {
      expect(mockInjectChaos).toHaveBeenCalledWith('stream-001', 'HIGH_BUFFER', 30)
      expect(mockToastSuccess).toHaveBeenCalledWith(expect.stringContaining('Chaos injected'))
    })
  })

  it('shows countdown and disables selects when chaos is active', async () => {
    mockInjectChaos.mockResolvedValueOnce({ chaosId: 'chaos-456', startsAt: Date.now() })

    render(<ChaosButton streamId="stream-001" />)

    await act(async () => {
      fireEvent.click(screen.getByTestId('chaos-inject-btn'))
    })

    await waitFor(() => {
      // Inject button replaced by countdown + cancel
      expect(screen.queryByTestId('chaos-inject-btn')).toBeNull()
      expect(screen.getByTestId('chaos-countdown')).toBeDefined()
      expect(screen.getByTestId('chaos-cancel-btn')).toBeDefined()
    })

    // Selects should be disabled.
    const scenarioSelect = screen.getByTestId('chaos-scenario-select') as HTMLSelectElement
    expect(scenarioSelect.disabled).toBe(true)
  })

  it('calls cancelChaos and shows success toast on cancel click', async () => {
    mockInjectChaos.mockResolvedValueOnce({ chaosId: 'chaos-789', startsAt: Date.now() })
    mockCancelChaos.mockResolvedValueOnce(undefined)

    render(<ChaosButton streamId="stream-001" />)

    // Inject first.
    await act(async () => {
      fireEvent.click(screen.getByTestId('chaos-inject-btn'))
    })

    await waitFor(() => screen.getByTestId('chaos-cancel-btn'))

    // Cancel.
    await act(async () => {
      fireEvent.click(screen.getByTestId('chaos-cancel-btn'))
    })

    await waitFor(() => {
      expect(mockCancelChaos).toHaveBeenCalledWith('stream-001', 'chaos-789')
      expect(mockToastSuccess).toHaveBeenCalledWith('Chaos cancelled')
    })
  })

  it('shows error toast when API returns an error', async () => {
    mockInjectChaos.mockRejectedValueOnce(new Error('500 Internal Server Error'))

    render(<ChaosButton streamId="stream-001" />)

    await act(async () => {
      fireEvent.click(screen.getByTestId('chaos-inject-btn'))
    })

    await waitFor(() => {
      expect(mockToastError).toHaveBeenCalledWith(expect.stringContaining('Failed to inject chaos'))
    })
  })

  it('shows countdown text in the countdown badge when active', async () => {
    // Countdown value is computed from endsAt; here we just verify the badge renders.
    mockInjectChaos.mockResolvedValueOnce({
      chaosId: 'chaos-timer',
      // Set startsAt in the future so countdown > 0 on first tick.
      startsAt: Date.now(),
    })

    render(<ChaosButton streamId="stream-001" />)

    await act(async () => {
      fireEvent.click(screen.getByTestId('chaos-inject-btn'))
    })

    await waitFor(() => {
      const badge = screen.getByTestId('chaos-countdown')
      // Badge text should contain a number followed by "s" or "—" fallback.
      expect(badge.textContent).toMatch(/\d+s|—/)
    })
  })
})
