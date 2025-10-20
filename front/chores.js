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

let state = { chores: [], fetching: false }

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

function fmtReqResources(c) {
    // configuration is a JSON string; expected keys: cpus, gpus
    try {
        const cfg = typeof c.configuration === 'string' ? JSON.parse(c.configuration) : (c.configuration || {})
        const cpus = Number(cfg?.cpus) || 0
        const gpus = Number(cfg?.gpus) || 0
        if (cpus === 0 && gpus === 0) return '-'
        return `${cpus} CPU · ${gpus} GPU`
    } catch {
        return '-'
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
    for (const c of chores) els.tbody.appendChild(createRow(c))
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
    // reorder by status priority, then ID desc
    const rows = [...els.tbody.querySelectorAll('.chore-row')]
    rows.sort((ra, rb) => {
        const a = map.get(ra.dataset.id)
        const b = map.get(rb.dataset.id)
        const pa = statusPriority(a?.Status)
        const pb = statusPriority(b?.Status)
        if (pa !== pb) return pa - pb
        return Number(b?.ID || 0) - Number(a?.ID || 0)
    })
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

els.refreshBtn.addEventListener('click', () => fetchIncremental())
fetchInitial()
const UPDATE_MS = 5000
setInterval(fetchIncremental, UPDATE_MS)
