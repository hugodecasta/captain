// chores.js (ES module)
// Smooth, incremental chores list backed by /api/chores/

const API = { chores: '/api/chores/' }

const els = {
    tbody: document.getElementById('tbody'),
    count: document.getElementById('count'),
    status: document.getElementById('status'),
    refreshBtn: document.getElementById('refreshBtn'),
    autoToggle: document.getElementById('autoToggle'),
}

let state = { chores: [], fetching: false, sort: null }

function setStatus(text) { els.status.textContent = text }

function statusClass(s) {
    switch (s) {
        case 'RUNNING': return 'warn'          // keep yellow
        case 'FAILED': return 'fail'           // red
        case 'ASSIGNED': return 'warn-light'   // light yellow
        case 'COMPLETED': return 'ok'          // green
        case 'PENDING': return 'pending'       // gray
        case 'CANCELED': return 'down'         // grayish
        default: return ''
    }
}

function statusPriority(s) {
    // Order: Running, Failed, Assigned, Completed, Pending
    switch (s) {
        case 'RUNNING': return 0
        case 'FAILED': return 1
        case 'ASSIGNED': return 2
        case 'COMPLETED': return 3
        case 'PENDING': return 4
        case 'CANCELED': return 5
        default: return 6
    }
}

function fmtTime(t) {
    if (t == null) return '-'
    try {
        return new Date((typeof t === 'number' ? t : Number(t)) * 1000).toLocaleString()
    } catch { return String(t) }
}

// Helpers for resources parsing/sorting
function parseReqResources(c) {
    try {
        const cfg = typeof c.configuration === 'string' ? JSON.parse(c.configuration) : (c.configuration || {})
        return { cpus: Number(cfg?.cpus) || 0, gpus: Number(cfg?.gpus) || 0 }
    } catch {
        return { cpus: 0, gpus: 0 }
    }
}

function fmtReqResources(c) {
    const { cpus, gpus } = parseReqResources(c)
    if (cpus === 0 && gpus === 0) return '-'
    return `${cpus} CPU · ${gpus} GPU`
}

// Sorting helpers
const COL_KEYS = ['id', 'status', 'owner', 'req', 'reqres', 'sailor', 'pid', 'infos', 'start', 'end']

function getSortValue(c, key) {
    switch (key) {
        case 'id': return Number(c.ID) || 0
        case 'status': return statusPriority(c.Status)
        case 'owner': return (c.owner ?? '').toLowerCase()
        case 'req': return (c.RService ?? c.RSailor ?? '').toLowerCase()
        case 'reqres': {
            const { cpus, gpus } = parseReqResources(c)
            return [cpus, gpus]
        }
        case 'sailor': return (c.Sailor ?? '').toLowerCase()
        case 'pid': return Number(c.PID) || 0
        case 'infos': return (c.Infos ?? '').toLowerCase()
        case 'start': return Number(c.Start) || 0
        case 'end': return Number(c.End) || 0
        default: return ''
    }
}

function makeComparator(sort) {
    return (a, b) => {
        let va = getSortValue(a, sort.key)
        let vb = getSortValue(b, sort.key)
        let cmp = 0
        if (Array.isArray(va) && Array.isArray(vb)) {
            // compare cpus then gpus
            cmp = va[0] - vb[0]
            if (cmp === 0) cmp = va[1] - vb[1]
        } else if (typeof va === 'number' && typeof vb === 'number') {
            cmp = va - vb
        } else {
            cmp = String(va).localeCompare(String(vb))
        }
        if (sort.dir === 'desc') cmp = -cmp
        // stable-ish tie-breaker by ID desc (newer first)
        if (cmp === 0) cmp = (Number(b?.ID || 0) - Number(a?.ID || 0))
        return cmp
    }
}

function createRow(c) {
    const tr = document.createElement('tr')
    tr.className = 'chore-row'
    tr.dataset.id = String(c.ID)
    tr.innerHTML = `
        <td class="mono">#${c.ID}</td>
        <td><span class="badge ${statusClass(c.Status)}">${c.Status ?? '-'}</span></td>
        <td class="mono owner">${c.owner ?? '-'}</td>
        <td class="mono req">${c.RService ?? c.RSailor ?? '-'}</td>
            <td class="mono reqres">${fmtReqResources(c)}</td>
        <td class="mono sailor">${c.Sailor ?? '-'}</td>
        <td class="mono pid">${c.PID ?? '-'}</td>
        <td class="mono infos">${c.Infos ?? '-'}</td>
        <td class="mono start">${fmtTime(c.Start)}</td>
        <td class="mono end">${fmtTime(c.End)}</td>
    `
    return tr
}

function updateRow(tr, c) {
    const set = (sel, val) => { const el = tr.querySelector(sel); if (el) el.textContent = val }
    const badge = tr.querySelector('.badge')
    if (badge) {
        badge.classList.remove('ok', 'warn', 'down')
        badge.classList.add(statusClass(c.Status))
        badge.textContent = c.Status ?? '-'
    }
    set('.owner', c.owner ?? '-')
    set('.req', c.RService ?? c.RSailor ?? '-')
    set('.reqres', fmtReqResources(c))
    set('.sailor', c.Sailor ?? '-')
    set('.pid', c.PID ?? '-')
    set('.infos', c.Infos ?? '-')
    set('.start', fmtTime(c.Start))
    set('.end', fmtTime(c.End))
}

function buildTable(chores) {
    els.tbody.innerHTML = ''
    const list = state.sort ? [...chores].sort(makeComparator(state.sort)) : chores
    for (const c of list) els.tbody.appendChild(createRow(c))
    els.count.textContent = String(chores.length)
}

function patchTable(chores) {
    const map = new Map(chores.map(c => [String(c.ID), c]))
    // remove missing
    for (const tr of Array.from(els.tbody.querySelectorAll('.chore-row'))) {
        if (!map.has(tr.dataset.id)) tr.remove()
    }
    // update/add
    for (const c of chores) {
        let tr = els.tbody.querySelector(`.chore-row[data-id="${CSS.escape(String(c.ID))}"]`)
        if (!tr) {
            els.tbody.appendChild(createRow(c))
        } else {
            updateRow(tr, c)
        }
    }
    // reorder rows
    const rows = [...els.tbody.querySelectorAll('.chore-row')]
    if (state.sort) {
        const cmp = makeComparator(state.sort)
        rows.sort((ra, rb) => {
            const a = map.get(ra.dataset.id)
            const b = map.get(rb.dataset.id)
            return cmp(a ?? {}, b ?? {})
        })
    } else {
        // default: by status priority, then ID desc (keeps previous behavior)
        rows.sort((ra, rb) => {
            const a = map.get(ra.dataset.id)
            const b = map.get(rb.dataset.id)
            const pa = statusPriority(a?.Status)
            const pb = statusPriority(b?.Status)
            if (pa !== pb) return pa - pb
            return Number(b?.ID || 0) - Number(a?.ID || 0)
        })
    }
    for (const tr of rows) els.tbody.appendChild(tr)
    els.count.textContent = String(chores.length)
}

async function fetchInitial() {
    setStatus('Loading chores…')
    try {
        const res = await fetch(API.chores, { cache: 'no-store' })
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
        const data = await res.json()
        state.chores = Array.isArray(data) ? data : []
        // sort ID desc initially
        state.chores.sort((a, b) => Number(b.ID) - Number(a.ID))
        buildTable(state.chores)
        setStatus(`Loaded ${state.chores.length} chores.`)
    } catch (e) {
        console.error(e)
        setStatus('Failed to load chores.')
    }
}

async function fetchIncremental() {
    if (state.fetching || !els.autoToggle.checked) return
    state.fetching = true
    try {
        const res = await fetch(API.chores, { cache: 'no-store' })
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
        const data = await res.json()
        if (Array.isArray(data)) {
            // keep sort consistent
            data.sort((a, b) => Number(b.ID) - Number(a.ID))
            state.chores = data
            patchTable(state.chores)
            setStatus(`Updated ${state.chores.length} chores · ${new Date().toLocaleTimeString()}`)
        }
    } catch (e) {
        console.debug('Incremental chores fetch failed:', e)
    } finally {
        state.fetching = false
    }
}

function initSorting() {
    const table = els.tbody?.closest('table')
    const headers = table?.querySelectorAll('thead th')
    if (!headers || headers.length === 0) return
    headers.forEach((th, idx) => {
        const key = th.dataset?.key || COL_KEYS[idx]
        if (!key) return
        th.style.cursor = 'pointer'
        th.addEventListener('click', () => {
            if (state.sort && state.sort.key === key) {
                state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc'
            } else {
                state.sort = { key, dir: 'asc' }
            }
            patchTable(state.chores)
        })
    })
}

els.refreshBtn.addEventListener('click', () => fetchIncremental())
fetchInitial()
const UPDATE_MS = 5000
setInterval(fetchIncremental, UPDATE_MS)
initSorting()
