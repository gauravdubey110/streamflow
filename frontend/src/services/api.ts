import axios from 'axios'

// Base URL is read from the VITE_API_BASE env variable (set in .env).
// Falls back to the API gateway default for local development.
const api = axios.create({
  baseURL: import.meta.env['VITE_API_BASE'] ?? 'http://localhost:8080',
  headers: {
    'Content-Type': 'application/json',
  },
})

export default api
