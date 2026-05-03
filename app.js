/* ================================================================
   EcoCart — Frontend Application Logic
   Mirrors CineMatch app.js pattern
   ================================================================ */

  // const API = 'http://localhost:8000';
const API = "https://your-app.onrender.com";
// ── State ──────────────────────────────────────────────────────────────
const state = {
  sessionId:   null,
  page:        'choose_category',
  category:    null,
  values:      new Set(),
  intentText:  '',
  budgetLevel: 'any',
  maxPrice:    null,
  results:     [],
  interacted:  {},
  stats:       { seen:0, liked:0, disliked:0, interactions:0 },
  isColdStart: true,
  topTags:     [],
};

// ── Eco values config ──────────────────────────────────────────────────
const ECO_VALUES = [
  { id: 'zero waste',    label: 'Zero Waste',      icon: '♻️' },
  { id: 'organic',       label: 'Organic',          icon: '🌾' },
  { id: 'vegan',         label: 'Vegan',            icon: '🌱' },
  { id: 'plastic free',  label: 'Plastic-Free',     icon: '🚫' },
  { id: 'fair trade',    label: 'Fair Trade',        icon: '🤝' },
  { id: 'recycled',      label: 'Recycled',          icon: '🔄' },
  { id: 'sustainable',   label: 'Sustainable',       icon: '🌍' },
  { id: 'natural',       label: 'Natural',           icon: '🍃' },
  { id: 'refillable',    label: 'Refillable',        icon: '🫙' },
  { id: 'durable',       label: 'Durable',           icon: '💪' },
  { id: 'cruelty free',  label: 'Cruelty-Free',      icon: '🐰' },
  { id: 'energy saving', label: 'Energy Saving',     icon: '⚡' },
  { id: 'carbon neutral',label: 'Carbon Neutral',    icon: '🌫️' },
  { id: 'local',         label: 'Locally Made',      icon: '🏡' },
  { id: 'solar',         label: 'Solar',             icon: '☀️' },
  { id: 'waterless',     label: 'Waterless',         icon: '💧' },
  { id: 'compostable',   label: 'Compostable',       icon: '🍂' },
  { id: 'bamboo',        label: 'Bamboo',            icon: '🎋' },
  { id: 'secondhand',    label: 'Secondhand',        icon: '🔁' },
  { id: 'premium',       label: 'Premium',           icon: '✨' },
];

const ECO_FACTS = [
  '💡 Switching to a shampoo bar saves ~3 plastic bottles per year.',
  '🌊 Over 8M tonnes of plastic enter our oceans annually.',
  '🌱 Organic farming uses 50% less energy than conventional.',
  '🐄 Going vegan for one year saves 1 tonne of CO₂.',
  '♻️ Recycled aluminium uses 95% less energy to produce.',
  '💧 A refillable bottle saves ~156 plastic bottles per year.',
  '🌳 B-Corp companies must meet the highest social & environmental standards.',
  '🌾 Fair Trade premiums fund schools, hospitals and clean water.',
];

const CATEGORY_ICONS = {
  beauty: '✨', home: '🏠', food: '🥦', clothing: '👕',
  electronics: '☀️', outdoor: '🌿', gift: '🎁', all: '🌍',
  accessories: '👜', footwear: '👟', sport: '🏃',
};

// ── Init ──────────────────────────────────────────────────────────────
function init() {
  state.sessionId = 'eco-' + Math.random().toString(36).slice(2, 10);
  updateGreeting();
  renderValuesGrid();
  bindCategoryCards();
  bindValueChips();
  bindIntentInput();
  bindBudgetChips();
  bindButtons();
  updateStepBar();
  showEcoFact();
}

function updateGreeting() {
  const hour = new Date().getHours();
  const g = hour < 12 ? 'Good morning!' : hour < 17 ? 'Good afternoon!' : 'Good evening!';
  document.getElementById('greeting-text').textContent = g;
}

function showEcoFact() {
  const el = document.getElementById('eco-fact');
  if (el) el.textContent = ECO_FACTS[Math.floor(Math.random() * ECO_FACTS.length)];
}

// ── Step bar ──────────────────────────────────────────────────────────
const STEPS = ['Choose Category', 'Choose Values', 'Your Eco Picks'];
const PAGE_ORDER = ['choose_category', 'choose_values', 'results'];

function updateStepBar() {
  const bar  = document.getElementById('step-bar');
  const curr = PAGE_ORDER.indexOf(state.page);
  bar.innerHTML = STEPS.map((s, i) => {
    const cls = i < curr ? 'done' : i === curr ? 'active' : '';
    return `<span class="step-pill ${cls}">${i < curr ? '✓ ' : ''}${s}</span>`;
  }).join('');
}

// ── Navigation ────────────────────────────────────────────────────────
function goTo(page) {
  document.querySelector('.page.active')?.classList.remove('active');
  document.getElementById(`page-${page}`)?.classList.add('active');
  state.page = page;
  updateStepBar();
}

// ── Category cards ────────────────────────────────────────────────────
function bindCategoryCards() {
  document.querySelectorAll('.category-card').forEach(card => {
    card.addEventListener('click', () => {
      state.category = card.dataset.cat;
      document.getElementById('values-heading').textContent =
        `What matters for your ${state.category === 'all' ? 'shopping' : state.category}?`;
      goTo('choose_values');
    });
  });
}

// ── Values grid ───────────────────────────────────────────────────────
function renderValuesGrid() {
  const grid = document.getElementById('values-grid');
  grid.innerHTML = ECO_VALUES.map(v => `
    <div class="value-chip" data-value="${v.id}">
      <span class="value-chip-icon">${v.icon}</span>
      <span>${v.label}</span>
    </div>
  `).join('');
}

function bindValueChips() {
  document.getElementById('values-grid').addEventListener('click', e => {
    const chip = e.target.closest('.value-chip');
    if (!chip) return;
    const val = chip.dataset.value;
    if (state.values.has(val)) {
      state.values.delete(val);
      chip.classList.remove('selected');
    } else {
      state.values.add(val);
      chip.classList.add('selected');
    }
    updateFindButton();
  });
}

function bindIntentInput() {
  const input = document.getElementById('intent-text-input');
  input.addEventListener('input', () => {
    state.intentText = input.value.trim();
    updateFindButton();
  });
}

function bindBudgetChips() {
  document.querySelectorAll('.budget-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      document.querySelectorAll('.budget-chip').forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      state.budgetLevel = chip.dataset.budget;
    });
  });
  document.getElementById('max-price-input').addEventListener('input', e => {
    state.maxPrice = e.target.value ? parseFloat(e.target.value) : null;
  });
}

function updateFindButton() {
  const btn = document.getElementById('btn-find');
  btn.disabled = state.values.size === 0 && !state.intentText;
}

function bindButtons() {
  document.getElementById('btn-back-values').addEventListener('click', () => goTo('choose_category'));
  document.getElementById('btn-find').addEventListener('click', fetchRecommendations);
  document.getElementById('btn-discover').addEventListener('click', () => fetchRecommendations(true));
  document.getElementById('btn-refresh').addEventListener('click', fetchRecommendations);
  document.getElementById('btn-change-values').addEventListener('click', () => goTo('choose_values'));
  document.getElementById('btn-start-over').addEventListener('click', startOver);
}

// ── API calls ──────────────────────────────────────────────────────────
async function fetchRecommendations(surpriseMe = false) {
  goTo('results');
  const loading = document.getElementById('loading-screen');
  const content = document.getElementById('results-content');
  loading.style.display = 'flex';
  content.style.display = 'none';

  const intentParts = [...state.values].join(' ') + ' ' + state.intentText;

  try {
    const res = await fetch(`${API}/recommend`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id:   state.sessionId,
        intent_text:  surpriseMe ? '' : intentParts.trim(),
        surprise_me:  !!surpriseMe,
        top_n:        12,
        budget_level: state.budgetLevel,
        max_price:    state.maxPrice,
        category:     state.category === 'all' ? null : state.category,
      })
    });
    const data = await res.json();
    state.results     = data.results || [];
    state.isColdStart = data.is_cold_start;
    state.topTags     = data.top_tags || [];
    state.stats       = data.stats || state.stats;
    state.interacted  = {};

    renderResults(surpriseMe);
    updateSidebar();
  } catch (err) {
    showToast('Could not connect to EcoCart backend. Is it running?', 'error');
    loading.style.display = 'none';
  }
}

async function sendInteraction(productId, tagList, category, liked) {
  try {
    const res = await fetch(`${API}/interact`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id:  state.sessionId,
        product_id:  productId,
        tag_list:    tagList,
        category:    category,
        liked:       liked,
      })
    });
    const data = await res.json();
    state.isColdStart = data.is_cold_start;
    state.topTags     = data.top_tags || [];
    state.stats       = data.stats || state.stats;
    updateSidebar();
  } catch (err) {
    // silent
  }
}

// ── Render results ─────────────────────────────────────────────────────
function renderResults(surpriseMe) {
  const loading = document.getElementById('loading-screen');
  const content = document.getElementById('results-content');
  const list    = document.getElementById('results-list');
  const title   = document.getElementById('result-banner-title');
  const badge   = document.getElementById('result-badge');
  const persTag = document.getElementById('personalised-tag');

  loading.style.display = 'none';
  content.style.display = 'block';

  const catLabel = state.category === 'all' || !state.category
    ? 'Eco Products' : state.category.charAt(0).toUpperCase() + state.category.slice(1);

  title.textContent = surpriseMe ? 'Discover Something New' : 'Your Eco Picks';
  badge.textContent = `${CATEGORY_ICONS[state.category] || '🌿'} ${catLabel}`;
  persTag.style.display = !state.isColdStart ? 'block' : 'none';

  if (!state.results.length) {
    list.innerHTML = '<div style="color:var(--text-muted);font-size:.85rem;padding:40px 0">No products found. Try different values or explore all categories.</div>';
    return;
  }

  list.innerHTML = state.results.map(p => renderProductCard(p)).join('');

  // Bind action buttons
  list.querySelectorAll('.action-btn').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const card  = btn.closest('.product-card');
      const pid   = card.dataset.id;
      const liked = btn.dataset.action === 'like';
      const prod  = state.results.find(r => r.product_id === pid);
      if (!prod) return;

      if (state.interacted[pid] === (liked ? 'like' : 'dislike')) return;
      state.interacted[pid] = liked ? 'like' : 'dislike';

      card.querySelectorAll('.action-btn').forEach(b => b.classList.remove('liked','disliked'));
      btn.classList.add(liked ? 'liked' : 'disliked');

      sendInteraction(pid, prod.tag_list || [], prod.category || '', liked);
      showToast(liked ? '💚 Added to loved picks!' : '👎 We\'ll show less of this', liked ? 'success' : '');
    });
  });
}

function renderProductCard(p) {
  const ecoScore  = p.eco_score ? parseFloat(p.eco_score).toFixed(1) : null;
  const ecoColor  = ecoScore >= 9 ? '#52b788' : ecoScore >= 7 ? '#b7e4c7' : '#8b6f47';
  const priceStr  = p.price ? `$${parseFloat(p.price).toFixed(2)}` : null;
  const catIcon   = CATEGORY_ICONS[p.category] || '🌿';

  const tags     = (p.tag_list || []).slice(0, 4);
  const certs    = (p.certifications || []).slice(0, 3);
  const imgEmoji = catIcon;

  return `
  <div class="product-card" data-id="${esc(p.product_id)}">
    <div class="product-image-wrap">
      ${p.image_url
        ? `<img src="${esc(p.image_url)}" alt="${esc(p.name)}" loading="lazy" onerror="this.parentElement.innerHTML='<div class=product-image-placeholder>${imgEmoji}</div>'">`
        : `<div class="product-image-placeholder">${imgEmoji}</div>`
      }
      ${ecoScore ? `
        <div class="eco-score-badge">
          <span class="eco-score-dot" style="background:${ecoColor};box-shadow:0 0 6px ${ecoColor}"></span>
          Eco ${ecoScore}
        </div>` : ''}
    </div>
    <div class="product-body">
      <div class="product-category">${catIcon} ${p.category || 'general'}</div>
      <div class="product-name">${esc(p.name)}</div>
      ${p.brand ? `<div class="product-brand">by ${esc(p.brand)}</div>` : ''}
      ${certs.length ? `<div class="cert-chips">${certs.map(c => `<span class="cert-chip-sm">${esc(c)}</span>`).join('')}</div>` : ''}
      <div class="product-desc">${esc(p.description)}</div>
      ${tags.length ? `<div class="product-tags">${tags.map(t => `<span class="product-tag">${esc(t)}</span>`).join('')}</div>` : ''}
      <div class="product-footer">
        ${priceStr
          ? `<div class="product-price">${priceStr}</div>`
          : `<div class="product-price-na">Price varies</div>`}
        <div class="product-actions">
          <button class="action-btn" data-action="like"    title="Love it">💚</button>
          <button class="action-btn" data-action="dislike" title="Not for me">👎</button>
          ${p.product_url ? `<a class="action-btn" href="${esc(p.product_url)}" target="_blank" title="View product" style="text-decoration:none">🔗</a>` : ''}
        </div>
      </div>
    </div>
  </div>`;
}

// ── Sidebar updates ────────────────────────────────────────────────────
function updateSidebar() {
  document.getElementById('stat-seen').textContent     = state.stats.seen     || 0;
  document.getElementById('stat-liked').textContent    = state.stats.liked    || 0;
  document.getElementById('stat-disliked').textContent = state.stats.disliked || 0;

  const profileEl = document.getElementById('taste-profile');

  if (state.isColdStart) {
    profileEl.innerHTML = `
      <div class="cold-start-box">
        <strong>🌿 Rate ${5 - (state.stats.interactions || 0)} more products</strong><br/>
        to unlock personalised eco picks
      </div>`;
  } else {
    const tags = state.topTags.slice(0, 6);
    profileEl.innerHTML = `
      <div style="margin-bottom:8px;font-size:.68rem;color:var(--text-muted)">Your top eco values:</div>
      <div class="profile-tags">
        ${tags.map(t => `<span class="profile-tag">${t}</span>`).join('')}
      </div>`;
  }
}

// ── Start over ─────────────────────────────────────────────────────────
function startOver() {
  state.values.clear();
  state.intentText  = '';
  state.budgetLevel = 'any';
  state.maxPrice    = null;
  state.category    = null;

  document.querySelectorAll('.value-chip.selected').forEach(c => c.classList.remove('selected'));
  document.querySelectorAll('.budget-chip').forEach(c => c.classList.toggle('active', c.dataset.budget === 'any'));
  document.getElementById('intent-text-input').value = '';
  document.getElementById('max-price-input').value   = '';
  document.getElementById('btn-find').disabled = true;

  showEcoFact();
  goTo('choose_category');
}

// ── Toast ──────────────────────────────────────────────────────────────
function showToast(msg, type = '') {
  const tc   = document.getElementById('toast-container');
  const el   = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  tc.appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

// ── Utils ──────────────────────────────────────────────────────────────
function esc(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;');
}

// ── Boot ──────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);
