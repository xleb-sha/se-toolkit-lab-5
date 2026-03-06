import { useState, useEffect } from 'react'
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
    LineElement,
    PointElement,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
    LineElement,
    PointElement,
)

const STORAGE_KEY = 'api_key'

interface ScoreBucket {
    bucket: string
    count: number
}

interface TimelineEntry {
    date: string
    submissions: number
}

interface PassRate {
    task: string
    avg_score: number
    attempts: number
}

interface Lab {
    id: number
    type: string
    title: string
    created_at: string
}

type FetchState<T> =
    | { status: 'idle' }
    | { status: 'loading' }
    | { status: 'success'; data: T }
    | { status: 'error'; message: string }

function Dashboard() {
    const [token] = useState(() => localStorage.getItem(STORAGE_KEY) ?? '')
    const [selectedLab, setSelectedLab] = useState<string>('')
    const [labs, setLabs] = useState<FetchState<Lab[]>>({ status: 'idle' })
    const [scores, setScores] = useState<FetchState<ScoreBucket[]>>({ status: 'idle' })
    const [timeline, setTimeline] = useState<FetchState<TimelineEntry[]>>({ status: 'idle' })
    const [passRates, setPassRates] = useState<FetchState<PassRate[]>>({ status: 'idle' })

    // Fetch labs on mount
    useEffect(() => {
        if (!token) return

        setLabs({ status: 'loading' })
        fetch('/items/', {
            headers: { Authorization: `Bearer ${token}` },
        })
            .then((res) => {
                if (!res.ok) throw new Error(`HTTP ${res.status}`)
                return res.json()
            })
            .then((data: Lab[]) => {
                const labItems = data.filter((item) => item.type === 'lab')
                setLabs({ status: 'success', data: labItems })
                if (labItems.length > 0 && !selectedLab) {
                    setSelectedLab(labItems[0].title.toLowerCase().replace('lab ', 'lab-'))
                }
            })
            .catch((err) => {
                setLabs({ status: 'error', message: err.message })
            })
    }, [token, selectedLab])

    // Fetch analytics data when lab changes
    useEffect(() => {
        if (!token || !selectedLab) return

        const fetchData = async <T,>(endpoint: string, setter: (state: FetchState<T>) => void) => {
            setter({ status: 'loading' })
            try {
                const res = await fetch(`/analytics/${endpoint}?lab=${selectedLab}`, {
                    headers: { Authorization: `Bearer ${token}` },
                })
                if (!res.ok) throw new Error(`HTTP ${res.status}`)
                const data = await res.json()
                setter({ status: 'success', data })
            } catch (err) {
                setter({ status: 'error', message: (err as Error).message })
            }
        }

        fetchData('scores', setScores)
        fetchData('timeline', setTimeline)
        fetchData('pass-rates', setPassRates)
    }, [token, selectedLab])

    const handleLabChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
        setSelectedLab(event.target.value)
    }

    if (!token) {
        return <div>Please set your API key in localStorage.</div>
    }

    return (
        <div>
            <h1>Analytics Dashboard</h1>

            {labs.status === 'loading' && <div>Loading labs...</div>}
            {labs.status === 'error' && <div>Error loading labs: {labs.message}</div>}
            {labs.status === 'success' && (
                <div>
                    <label htmlFor="lab-select">Select Lab:</label>
                    <select id="lab-select" value={selectedLab} onChange={handleLabChange}>
                        {labs.data.map((lab) => (
                            <option key={lab.id} value={lab.title.toLowerCase().replace('lab ', 'lab-')}>
                                {lab.title}
                            </option>
                        ))}
                    </select>
                </div>
            )}

            <div style={{ display: 'flex', flexWrap: 'wrap' }}>
                <div style={{ width: '50%', minWidth: '400px' }}>
                    <h2>Score Distribution</h2>
                    {scores.status === 'loading' && <div>Loading scores...</div>}
                    {scores.status === 'error' && <div>Error loading scores: {scores.message}</div>}
                    {scores.status === 'success' && (
                        <Bar
                            data={{
                                labels: scores.data.map((bucket) => bucket.bucket),
                                datasets: [
                                    {
                                        label: 'Count',
                                        data: scores.data.map((bucket) => bucket.count),
                                        backgroundColor: 'rgba(75, 192, 192, 0.6)',
                                    },
                                ],
                            }}
                            options={{
                                responsive: true,
                                plugins: {
                                    legend: { position: 'top' as const },
                                    title: { display: true, text: 'Score Buckets' },
                                },
                            }}
                        />
                    )}
                </div>

                <div style={{ width: '50%', minWidth: '400px' }}>
                    <h2>Submissions Timeline</h2>
                    {timeline.status === 'loading' && <div>Loading timeline...</div>}
                    {timeline.status === 'error' && <div>Error loading timeline: {timeline.message}</div>}
                    {timeline.status === 'success' && (
                        <Line
                            data={{
                                labels: timeline.data.map((entry) => entry.date),
                                datasets: [
                                    {
                                        label: 'Submissions',
                                        data: timeline.data.map((entry) => entry.submissions),
                                        borderColor: 'rgba(255, 99, 132, 1)',
                                        backgroundColor: 'rgba(255, 99, 132, 0.2)',
                                    },
                                ],
                            }}
                            options={{
                                responsive: true,
                                plugins: {
                                    legend: { position: 'top' as const },
                                    title: { display: true, text: 'Daily Submissions' },
                                },
                            }}
                        />
                    )}
                </div>
            </div>

            <div>
                <h2>Pass Rates</h2>
                {passRates.status === 'loading' && <div>Loading pass rates...</div>}
                {passRates.status === 'error' && <div>Error loading pass rates: {passRates.message}</div>}
                {passRates.status === 'success' && (
                    <table>
                        <thead>
                            <tr>
                                <th>Task</th>
                                <th>Average Score</th>
                                <th>Attempts</th>
                            </tr>
                        </thead>
                        <tbody>
                            {passRates.data.map((rate, index) => (
                                <tr key={index}>
                                    <td>{rate.task}</td>
                                    <td>{rate.avg_score.toFixed(1)}</td>
                                    <td>{rate.attempts}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    )
}

export default Dashboard