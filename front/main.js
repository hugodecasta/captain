(function () {
    const qs = s => document.querySelector(s)
    const qsa = s => Array.from(document.querySelectorAll(s))
    const state = { token: null }

    function switchView(id) {
        qsa('.view').forEach(v => v.classList.add('hidden'))
        qs('#' + id).classList.remove('hidden')
    }

    function switchTab(name) {
        qsa('.tab').forEach(t => t.classList.remove('active'))
        const btn = qsa('.tab').find(t => t.dataset.tab === name)
        if (btn) btn.classList.add('active')
        qs('#cluster-sailors').classList.toggle('hidden', name !== 'sailors')
        qs('#cluster-services').classList.toggle('hidden', name !== 'services')
    }

    function statusColor(s) {
        s = (s || '').toLowerCase()
        if (s === 'idle') return 'green'
        if (s === 'busy') return 'yellow'
        if (s === 'full') return 'red'
        if (s === 'down') return 'grey'
        return 'grey'
    }

    async function fetchJSON(url, opts = {}) {
        const r = await fetch(url, opts)
        if (!r.ok) {
            let msg = await r.text()
            try {
                const ct = r.headers.get('content-type') || ''
                if (ct.includes('application/json')) {
                    const j = JSON.parse(msg)
                    msg = j.detail || j.message || JSON.stringify(j)
                }
            } catch { }
            const err = new Error(msg)
            err.status = r.status
            throw err
        }
        return r.json()
    }

    async function renderSailors() {
        const container = qs('#cluster-sailors')
        container.innerHTML = '<div class="grid"></div>'
        const grid = container.querySelector('.grid')
        const crew = await fetchJSON('/crew')
        crew.forEach(s => {
            const div = document.createElement('div')
            div.className = `square ${statusColor(s.derived_status || s.derived_status)}`
            div.innerHTML = `
        <div class="name">${s.name || '?'} </div>
        <div class="status">${s.derived_status || '-'}</div>
        <div class="tooltip">
          <div><b>${s.name || ''}</b> @ ${s.ip || '?'}:${s.port || 8001}</div>
          <div>Services: ${(Array.isArray(s.services) ? s.services.join(',') : s.services) || '-'}</div>
          <div>CPUs: ${s.used_cpus || 0}/${s.cpus || 0}</div>
          <div>GPUs: ${s.used_gpus || 0}/${(s.gpus || []).length}</div>
          <div>RAM: ${s.ram || 0}</div>
          <div>Last seen: ${s.last_seen || 0}</div>
        </div>`
            grid.appendChild(div)
        })
        attachTooltips(container)
    }

    async function renderServices() {
        const container = qs('#cluster-services')
        container.innerHTML = ''
        const crew = await fetchJSON('/crew')
        const bySvc = {}
        crew.forEach(s => {
            let svcs = s.services
            if (typeof svcs === 'string') svcs = svcs.split(',').map(x => x.trim()).filter(Boolean);
            (svcs || ['-']).forEach(sv => {
                (bySvc[sv] = bySvc[sv] || []).push(s)
            })
        })
        Object.entries(bySvc).forEach(([svc, list]) => {
            const section = document.createElement('section')
            section.innerHTML = `<h3>${svc}</h3><div class="grid"></div>`
            const grid = section.querySelector('.grid')
            list.forEach(s => {
                const div = document.createElement('div')
                div.className = `square ${statusColor(s.status || s.derived_status)}`
                div.innerHTML = `
                                    <div class="name">${s.name || '?'} </div>
                                    <div class="status">${s.status || '-'}</div>
                                    <div class="tooltip">
                                        <div><b>${s.name || ''}</b> @ ${s.ip || '?'}:${s.port || 8001}</div>
                                        <div>Services: ${(Array.isArray(s.services) ? s.services.join(',') : s.services) || '-'}</div>
                                        <div>CPUs: ${s.used_cpus || 0}/${s.cpus || 0}</div>
                                        <div>GPUs: ${s.used_gpus || 0}/${(s.gpus || []).length}</div>
                                        <div>RAM: ${s.ram || 0}</div>
                                        <div>Last seen: ${s.last_seen || 0}</div>
                                    </div>`
                grid.appendChild(div)
            })
            container.appendChild(section)
        })
        attachTooltips(container)
    }

    async function refreshChores() {
        const tbody = qs('#chores-table tbody')
        tbody.innerHTML = ''
        const headers = state.token ? { 'Authorization': 'Bearer ' + state.token } : {}
        const chores = await fetchJSON('/me/chores', { headers })
        chores.forEach(c => {
            const tr = document.createElement('tr')
            const res = c.ressources || {}
            const btn = document.createElement('button')
            btn.className = 'small'
            btn.textContent = 'Cancel'
            btn.onclick = async () => {
                try {
                    await fetchJSON('/me/cancel', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', ...headers },
                        body: JSON.stringify({ chore_id: c.chore_id })
                    })
                    await refreshChores()
                } catch (e) { alert('Cancel failed: ' + e) }
            }
            tr.innerHTML = `
        <td>${c.chore_id}</td>
        <td>${c.status || '-'}</td>
        <td>${c.sailor || '-'}</td>
        <td>${res.cpus || 0}</td>
        <td>${res.gpus || 0}</td>
        <td>${c.reason || '-'}</td>
        <td></td>`
            tr.lastElementChild.appendChild(btn)
            tbody.appendChild(tr)
        })
    }

    // Events
    qs('#btn-cluster').addEventListener('click', async () => {
        switchView('view-cluster')
        await renderSailors()
        await renderServices()
    })
    qs('#btn-chores').addEventListener('click', () => {
        switchView('view-chores')
    })
    qsa('.tab').forEach(t => t.addEventListener('click', async () => {
        switchTab(t.dataset.tab)
        if (t.dataset.tab === 'sailors') await renderSailors()
        if (t.dataset.tab === 'services') await renderServices()
    }))

    qs('#login-form').addEventListener('submit', async (e) => {
        e.preventDefault()
        const fd = new FormData(e.target)
        const creds = Object.fromEntries(fd.entries())
        const msg = qs('#login-msg')
        msg.textContent = ''
        try {
            const data = await fetchJSON('/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(creds)
            })
            state.token = data.token
            qs('#login-panel').classList.add('hidden')
            qs('#chores-panel').classList.remove('hidden')
            await refreshChores()
        } catch (err) {
            msg.textContent = 'Login failed: ' + (err && err.message ? err.message : 'Unknown error')
        }
    })

    qs('#refresh-chores').addEventListener('click', refreshChores)

    // Default view
    switchView('view-cluster')
    switchTab('sailors')
    renderSailors()
    renderServices()
    // Attach tooltip logic for initial render (sailors view)
    attachTooltips(document)

    // Tooltip positioning helpers
    function attachTooltips(root) {
        const nodes = (root === document) ? qsa('.square') : Array.from(root.querySelectorAll('.square'))
        nodes.forEach(el => {
            const tt = el.querySelector('.tooltip')
            if (!tt) return
            el.addEventListener('mouseenter', () => {
                tt.style.display = 'block'
                // delay to ensure size computed
                requestAnimationFrame(() => positionTooltip(el, tt))
            })
            el.addEventListener('mousemove', () => positionTooltip(el, tt))
            el.addEventListener('mouseleave', () => {
                tt.style.display = 'none'
                tt.style.left = ''
                tt.style.top = ''
                tt.style.bottom = ''
            })
        })
    }

    function positionTooltip(el, tt) {
        const rect = el.getBoundingClientRect()
        // choose above if space, else below
        const ttHeight = tt.offsetHeight || 0
        const aboveOK = (rect.top - ttHeight - 8) >= 0
        if (aboveOK) {
            tt.style.top = ''
            tt.style.bottom = `${rect.height + 8}px`
        } else {
            tt.style.bottom = ''
            tt.style.top = `${rect.height + 8}px`
        }
        // horizontal clamp within viewport
        const ttWidth = tt.offsetWidth || 0
        let x = rect.left + rect.width / 2 - ttWidth / 2
        const minX = 8
        const maxX = Math.max(minX, window.innerWidth - ttWidth - 8)
        x = Math.max(minX, Math.min(x, maxX))
        // set left relative to square
        tt.style.left = `${x - rect.left}px`
    }
})()
