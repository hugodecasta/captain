// main.js (ES module)
// Renders the cluster map in two modes: By Sailor and By Service

const API = {
    crew: '/api/crew/'
}

const els = {
    grid: document.getElementById('grid'),
    legend: document.getElementById('legend'),
    status: document.getElementById('status'),
    refreshBtn: document.getElementById('refreshBtn'),
    modeToggle: document.getElementById('modeToggle'),
    modeLabel: document.getElementById('modeLabel'),
}

const Mode = {
    Sailor: 'sailor',
    Service: 'service',
}

let state = {
    mode: Mode.Sailor,
    crew: [],
    fetching: false,
}

// Local storage helpers
const MODE_STORAGE_KEY = 'captain.mode'
function loadMode() {
    try {
        const m = localStorage.getItem(MODE_STORAGE_KEY)
        if (m === Mode.Sailor || m === Mode.Service) return m
    } catch { }
    return Mode.Sailor
}
function saveMode(mode) {
    try { localStorage.setItem(MODE_STORAGE_KEY, mode) } catch { }
}

function setStatus(text) {
    els.status.textContent = text
}

function statusDot(status) {
    const cls = status === 'DOWN' ? 'down' : status === 'WORKING' ? 'warn' : 'ok'
    return `<span class="dot ${cls}" title="${status}"></span>`
}

function tag(text) { return `<span class="tag">${text}</span>` }

function kv(k, v) {
    return `<div class="kv"><span class="k">${k}</span><span class="v">${v}</span></div>`
}

function fmtResources(s) {
    const freeCPU = (s.CPUS ?? 0) - (s.UsedCPUS ?? 0)
    const freeGPU = (s.GPUS ?? 0) - (s.UsedGPUS ?? 0)
    return `${freeCPU}/${s.CPUS} CPU · ${freeGPU}/${s.GPUS} GPU · ${s.RAM} RAM`
}

// ========== Sailor mode: build and patch ==========
function sailorLegend() {
    els.legend.innerHTML = `
  <h3>Legend</h3>
  <div class="row"><span class="swatch" style="background: var(--ok)"></span> Ready</div>
  <div class="row"><span class="swatch" style="background: var(--warn)"></span> Working</div>
  <div class="row"><span class="swatch" style="background: var(--down)"></span> Down</div>
  <small>Showing each Sailor (node) with resources and services.</small>
  `
}

function createSailorCard(s) {
    const article = document.createElement('article')
    article.className = 'card'
    article.setAttribute('role', 'region')
    article.setAttribute('aria-label', `Sailor ${s.Name}`)
    article.dataset.sailor = s.Name
    article.innerHTML = `
    <div class="head">
    <span class="dot ${s.Status === 'DOWN' ? 'down' : s.Status === 'WORKING' ? 'warn' : 'ok'}" title="${s.Status}"></span>
    <div class="title">${s.Name}</div>
    <div class="meta">· <span class="last-seen">${new Date(s.LastSeen * 1000).toLocaleTimeString()}</span></div>
    </div>
    <div class="body">
    <div class="kv resources"><span class="k">Resources</span><span class="v">${fmtResources(s)}</span></div>
    <div class="kv status"><span class="k">Status</span><span class="v status-text">${s.Status}</span></div>
    <div class="tags" title="Services"></div>
    </div>
  `
    const tags = article.querySelector('.tags')
    for (const svc of (s.Services || [])) {
        const span = document.createElement('span')
        span.className = 'tag'
        span.textContent = svc
        tags.appendChild(span)
    }
    return article
}

function updateSailorCard(card, s) {
    // status dot
    const dot = card.querySelector('.head .dot')
    if (dot) {
        dot.classList.remove('ok', 'warn', 'down')
        dot.classList.add(s.Status === 'DOWN' ? 'down' : s.Status === 'WORKING' ? 'warn' : 'ok')
        dot.title = s.Status
    }
    // last seen
    const lastSeen = card.querySelector('.head .last-seen')
    if (lastSeen) lastSeen.textContent = new Date(s.LastSeen * 1000).toLocaleTimeString()
    // resources
    const res = card.querySelector('.body .resources .v')
    if (res) res.textContent = fmtResources(s)
    // status text
    const st = card.querySelector('.body .status .status-text')
    if (st) st.textContent = s.Status
    // services tags
    const tags = card.querySelector('.body .tags')
    if (tags) {
        const wanted = new Set((s.Services || []))
        // remove tags not present
        for (const el of Array.from(tags.children)) {
            if (!wanted.has(el.textContent)) tags.removeChild(el)
        }
        // add missing tags
        for (const svc of wanted) {
            if (![...tags.children].some(ch => ch.textContent === svc)) {
                const span = document.createElement('span')
                span.className = 'tag'
                span.textContent = svc
                tags.appendChild(span)
            }
        }
    }
}

function buildSailorGrid(crew) {
    sailorLegend()
    els.grid.innerHTML = ''
    const sorted = [...crew].sort((a, b) => a.Name.localeCompare(b.Name))
    for (const s of sorted) {
        els.grid.appendChild(createSailorCard(s))
    }
}

function patchSailorGrid(crew) {
    sailorLegend()
    const map = new Map(crew.map(s => [s.Name, s]))
    // remove cards no longer present
    for (const card of Array.from(els.grid.querySelectorAll('.card[data-sailor]'))) {
        if (!map.has(card.dataset.sailor)) card.remove()
    }
    // update existing and add missing
    for (const s of crew) {
        let card = els.grid.querySelector(`.card[data-sailor="${CSS.escape(s.Name)}"]`)
        if (!card) {
            card = createSailorCard(s)
            els.grid.appendChild(card)
        } else {
            updateSailorCard(card, s)
        }
    }
    // reorder cards alphabetically by sailor name (stable, minimal DOM moves)
    const namesSorted = [...map.keys()].sort((a, b) => a.localeCompare(b))
    for (const name of namesSorted) {
        const card = els.grid.querySelector(`.card[data-sailor="${CSS.escape(name)}"]`)
        if (card) els.grid.appendChild(card)
    }
}

function groupByService(crew) {
    const map = new Map()
    for (const s of crew) {
        for (const svc of (s.Services || [])) {
            if (!map.has(svc)) map.set(svc, [])
            map.get(svc).push(s)
        }
    }
    return map // Map<string, Sailor[]>
}

// ========== Service mode: build and patch ==========
function serviceLegend() {
    els.legend.innerHTML = `
    <h3>Legend</h3>
    <div class="row"><span class="swatch" style="background: var(--accent)"></span> Service</div>
    <div class="row"><span class="swatch" style="background: var(--ok)"></span> Ready Sailor</div>
    <div class="row"><span class="swatch" style="background: var(--warn)"></span> Working Sailor</div>
    <div class="row"><span class="swatch" style="background: var(--down)"></span> Down Sailor</div>
    <small>Grouping sailors by provided service.</small>
  `
}

function createServiceCard(svc, sailors) {
    const article = document.createElement('article')
    article.className = 'card'
    article.setAttribute('role', 'region')
    article.setAttribute('aria-label', `Service ${svc}`)
    article.dataset.service = svc

    const totalCPU = sailors.reduce((n, s) => n + (s.CPUS || 0), 0)
    const usedCPU = sailors.reduce((n, s) => n + (s.UsedCPUS || 0), 0)
    const totalGPU = sailors.reduce((n, s) => n + (s.GPUS || 0), 0)
    const usedGPU = sailors.reduce((n, s) => n + (s.UsedGPUS || 0), 0)

    article.innerHTML = `
      <div class="head">
        <span class="dot" style="background: var(--accent)"></span>
        <div class="title">${svc}</div>
        <div class="meta">· <span class="svc-count">${sailors.length}</span> sailor(s)</div>
      </div>
      <div class="body">
        <div class="kv capacity"><span class="k">Capacity</span><span class="v"><span class="cap-text">${(totalCPU - usedCPU)}/${totalCPU} CPU · ${(totalGPU - usedGPU)}/${totalGPU} GPU</span></span></div>
        <div class="svc-list"></div>
      </div>
    `
    const list = article.querySelector('.svc-list')
    const prio = (s) => (s.Status === 'WORKING' ? 0 : s.Status === 'READY' ? 1 : 2)
    const sailorsSorted = [...sailors].sort((a, b) => {
        const pa = prio(a), pb = prio(b)
        if (pa !== pb) return pa - pb
        return a.Name.localeCompare(b.Name)
    })
    for (const s of sailorsSorted) {
        list.appendChild(createServiceSailorRow(s))
    }
    return article
}

function createServiceSailorRow(s) {
    const row = document.createElement('div')
    row.className = 'kv svc-sailor'
    row.dataset.name = s.Name
    row.innerHTML = `
      <span class="k"><span class="dot ${s.Status === 'DOWN' ? 'down' : s.Status === 'WORKING' ? 'warn' : 'ok'}" title="${s.Status}"></span> ${s.Name}</span>
      <span class="v"><span class="res-text">${(s.CPUS - s.UsedCPUS)}/${s.CPUS} CPU · ${(s.GPUS - s.UsedGPUS)}/${s.GPUS} GPU</span></span>
    `
    return row
}

function updateServiceCard(card, sailors) {
    // header count
    const count = card.querySelector('.svc-count')
    if (count) count.textContent = String(sailors.length)
    // capacity
    const totalCPU = sailors.reduce((n, s) => n + (s.CPUS || 0), 0)
    const usedCPU = sailors.reduce((n, s) => n + (s.UsedCPUS || 0), 0)
    const totalGPU = sailors.reduce((n, s) => n + (s.GPUS || 0), 0)
    const usedGPU = sailors.reduce((n, s) => n + (s.UsedGPUS || 0), 0)
    const cap = card.querySelector('.cap-text')
    if (cap) cap.textContent = `${(totalCPU - usedCPU)}/${totalCPU} CPU · ${(totalGPU - usedGPU)}/${totalGPU} GPU`

    const list = card.querySelector('.svc-list')
    const map = new Map(sailors.map(s => [s.Name, s]))
    // remove rows not present
    for (const row of Array.from(list.querySelectorAll('.svc-sailor'))) {
        if (!map.has(row.dataset.name)) row.remove()
    }
    // update or add rows
    for (const s of sailors) {
        let row = list.querySelector(`.svc-sailor[data-name="${CSS.escape(s.Name)}"]`)
        if (!row) {
            list.appendChild(createServiceSailorRow(s))
        } else {
            const dot = row.querySelector('.dot')
            if (dot) {
                dot.classList.remove('ok', 'warn', 'down')
                dot.classList.add(s.Status === 'DOWN' ? 'down' : s.Status === 'WORKING' ? 'warn' : 'ok')
                dot.title = s.Status
            }
            const res = row.querySelector('.res-text')
            if (res) res.textContent = `${(s.CPUS - s.UsedCPUS)}/${s.CPUS} CPU · ${(s.GPUS - s.UsedGPUS)}/${s.GPUS} GPU`
        }
    }
    // reorder rows by status (WORKING, READY, DOWN) then by name
    const prio = (s) => (s.Status === 'WORKING' ? 0 : s.Status === 'READY' ? 1 : 2)
    const sailorsSorted = [...map.values()].sort((a, b) => {
        const pa = prio(a), pb = prio(b)
        if (pa !== pb) return pa - pb
        return a.Name.localeCompare(b.Name)
    })
    for (const s of sailorsSorted) {
        const row = list.querySelector(`.svc-sailor[data-name="${CSS.escape(s.Name)}"]`)
        if (row) list.appendChild(row)
    }
}

function buildServiceGrid(crew) {
    serviceLegend()
    els.grid.innerHTML = ''
    const grouped = groupByService(crew)
    const services = [...grouped.keys()].sort((a, b) => a.localeCompare(b))
    for (const svc of services) {
        els.grid.appendChild(createServiceCard(svc, grouped.get(svc)))
    }
}

function patchServiceGrid(crew) {
    serviceLegend()
    const grouped = groupByService(crew)
    const services = new Set(grouped.keys())
    // remove service cards no longer present
    for (const card of Array.from(els.grid.querySelectorAll('.card[data-service]'))) {
        if (!services.has(card.dataset.service)) card.remove()
    }
    // update or add
    for (const [svc, sailors] of grouped.entries()) {
        let card = els.grid.querySelector(`.card[data-service="${CSS.escape(svc)}"]`)
        if (!card) {
            card = createServiceCard(svc, sailors)
            els.grid.appendChild(card)
        } else {
            updateServiceCard(card, sailors)
        }
    }
    // reorder service cards alphabetically
    const ordered = [...services].sort((a, b) => a.localeCompare(b))
    for (const svc of ordered) {
        const card = els.grid.querySelector(`.card[data-service="${CSS.escape(svc)}"]`)
        if (card) els.grid.appendChild(card)
    }
}

function renderInitial() {
    if (state.mode === Mode.Sailor) buildSailorGrid(state.crew)
    else buildServiceGrid(state.crew)
}

function patchRender(newCrew) {
    if (state.mode === Mode.Sailor) patchSailorGrid(newCrew)
    else patchServiceGrid(newCrew)
}

async function fetchCrewInitial() {
    setStatus('Loading crew…')
    try {
        const res = await fetch(API.crew, { cache: 'no-store' })
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
        const data = await res.json()
        state.crew = (Array.isArray(data) ? data : [])
        setStatus(`Loaded ${state.crew.length} sailor(s).`)
        renderInitial()
    } catch (err) {
        console.error(err)
        setStatus('Failed to load crew. Using sample data.')
        state.crew = sampleCrew()
        renderInitial()
    }
}

async function fetchCrewIncremental() {
    if (state.fetching) return
    state.fetching = true
    try {
        const res = await fetch(API.crew, { cache: 'no-store' })
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
        const data = await res.json()
        if (Array.isArray(data)) {
            setStatus(`Updated ${data.length} sailor(s) · ${new Date().toLocaleTimeString()}`)
            state.crew = data
            patchRender(state.crew)
        }
    } catch (err) {
        // Keep previous UI, just log
        console.debug('Incremental fetch failed:', err)
    } finally {
        state.fetching = false
    }
}

function sampleCrew() {
    const now = Math.floor(Date.now() / 1000)
    return [
        { Name: 'alpha', Services: ['train', 'serve'], CPUS: 16, GPUS: 2, RAM: 64, LastSeen: now, UsedCPUS: 4, UsedGPUS: 1, Status: 'WORKING' },
        { Name: 'beta', Services: ['serve'], CPUS: 8, GPUS: 0, RAM: 32, LastSeen: now, UsedCPUS: 0, UsedGPUS: 0, Status: 'READY' },
        { Name: 'gamma', Services: ['train'], CPUS: 24, GPUS: 4, RAM: 128, LastSeen: now - 99999, UsedCPUS: 0, UsedGPUS: 0, Status: 'DOWN' },
    ]
}

function initUI() {
    // Toggle between modes
    els.modeToggle.addEventListener('change', () => {
        state.mode = els.modeToggle.checked ? Mode.Service : Mode.Sailor
        saveMode(state.mode)
        els.modeLabel.textContent = state.mode === Mode.Sailor ? 'By Sailor' : 'By Service'
        // Full rebuild on mode switch to reshape grid; subsequent updates are incremental
        renderInitial()
    })

    els.refreshBtn.addEventListener('click', () => fetchCrewIncremental())

    // Initialize mode from storage
    state.mode = loadMode()
    els.modeToggle.checked = state.mode === Mode.Service
    els.modeLabel.textContent = state.mode === Mode.Sailor ? 'By Sailor' : 'By Service'
}

initUI()
fetchCrewInitial()

// Background incremental updater (smooth, no full re-render)
const UPDATE_MS = 5000
setInterval(fetchCrewIncremental, UPDATE_MS)
