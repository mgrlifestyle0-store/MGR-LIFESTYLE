import { createServer } from 'node:http';
import { createReadStream, existsSync, mkdirSync } from 'node:fs';
import { dirname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

const root = dirname(fileURLToPath(import.meta.url));
const dataDirectory = process.env.MGR_LIFE_DATA_DIR ? resolve(process.env.MGR_LIFE_DATA_DIR) : join(root, 'data');
mkdirSync(dataDirectory, { recursive: true });
const database = new DatabaseSync(join(dataDirectory, 'mgr-life.sqlite'));
database.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA foreign_keys = ON;
  CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'customer')),
    password_salt TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS app_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data TEXT NOT NULL
  );
`);

const defaultState = {
  products: [
    { id: 101, n: 'Detergent Liquid 500gm', c: 'Home Cleaning Products', sub: 'Detergent Liquid', p: 0, cost: 0, e: '🧴', r: 5, stock: 20 },
    { id: 102, n: 'Dish Wash Liquid 500gm', c: 'Home Cleaning Products', sub: 'Dish Wash Liquid', p: 0, cost: 0, e: '🧴', r: 5, stock: 20 },
    { id: 103, n: 'Hand Wash Liquid 500gm', c: 'Home Cleaning Products', sub: 'Hand Wash Liquid', p: 0, cost: 0, e: '🧼', r: 5, stock: 20 },
    { id: 104, n: 'Car Wash Liquid 500gm', c: 'Home Cleaning Products', sub: 'Car Wash Liquid', p: 0, cost: 0, e: '🚗', r: 5, stock: 20 },
    { id: 105, n: 'Bathroom Cleaning 500gm', c: 'Home Cleaning Products', sub: 'Bathroom Cleaning', p: 0, cost: 0, e: '🚿', r: 5, stock: 20 },
    { id: 106, n: 'Toilet Cleaner Liquid 500gm', c: 'Home Cleaning Products', sub: 'Toilet Cleaner Liquid', p: 0, cost: 0, e: '🚽', r: 5, stock: 20 },
    { id: 107, n: 'Glass Cleaner Liquid 500gm', c: 'Home Cleaning Products', sub: 'Glass Cleaner Liquid', p: 0, cost: 0, e: '🪟', r: 5, stock: 20 },
    { id: 108, n: 'Floor Cleaner Liquid 500gm', c: 'Home Cleaning Products', sub: 'Floor Cleaner Liquid', p: 0, cost: 0, e: '🧹', r: 5, stock: 20 },
    { id: 109, n: 'Phynole Liquid 500gm/1 Liter', c: 'Home Cleaning Products', sub: 'Phynole Liquid', p: 0, cost: 0, e: '🧴', r: 5, stock: 20 },
    { id: 1, n: 'Premium Detergent Powder 1kg', c: 'Home Cleaning Products', sub: 'Detergent Powder', p: 199, cost: 120, e: '🧴', r: 4.8, stock: 50 },
    { id: 2, n: 'Quality Cotton Shirt', c: 'Clothes', p: 499, cost: 280, e: '👕', r: 4.7, stock: 30 },
    { id: 3, n: 'Luxury Perfume 100ml', c: 'Perfume', p: 899, cost: 500, e: '🧴', r: 4.9, stock: 20 },
    { id: 4, n: '24K Gold Plated Necklace', c: 'Jewellery', p: 1299, cost: 700, e: '💎', r: 4.7, stock: 15 },
    { id: 5, n: 'Beauty Cosmetics Kit', c: 'Cosmetics', p: 699, cost: 360, e: '💄', r: 4.8, stock: 25 },
    { id: 6, n: 'Premium Home Care Set', c: 'Home Care', p: 599, cost: 300, e: '🏠', r: 4.6, stock: 18 },
    { id: 7, n: 'Wellness Essentials', c: 'Wellness', p: 799, cost: 420, e: '🌿', r: 4.7, stock: 22 },
    { id: 8, n: 'Wireless Lifestyle Headphones', c: 'Electronics', p: 1599, cost: 900, e: '🎧', r: 4.6, stock: 12 }
  ],
  cats: [
    { c: 'Home Cleaning Products', icon: '🧽', label: 'Home Cleaning' },
    { c: 'Clothes', icon: '👕', label: 'Clothes' },
    { c: 'Perfume', icon: '🧴', label: 'Perfume' },
    { c: 'Candles', icon: '🕯️', label: 'Candles' },
    { c: 'Jewellery', icon: '💎', label: 'Jewellery' },
    { c: 'Cosmetics', icon: '💄', label: 'Cosmetics' },
    { c: 'Home Care', icon: '🏠', label: 'Home Care' },
    { c: 'Wellness', icon: '🌿', label: 'Wellness' },
    { c: 'Electronics', icon: '🎧', label: 'Electronics' }
  ],
  subs: [],
  partners: [
    { name: 'Karan', share: 40, paid: 0 },
    { name: 'Reena', share: 30, paid: 0 },
    { name: 'Partner 3', share: 30, paid: 0 }
  ],
  orders: [],
  expenses: [],
  purchases: [],
  customers: [],
  settings: { firm: 'MGR LIFESTYLE', mobile: '7624006324', email: 'MGRLIFESTLYE0@gmail.com', free: 999 }
};

const readState = database.prepare('SELECT data FROM app_state WHERE id = 1');
const writeState = database.prepare('INSERT INTO app_state (id, data) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data');
if (!readState.get()) writeState.run(JSON.stringify(defaultState));

const sessions = new Map();
const loginFailures = new Map();
const sessionLifetimeMs = 8 * 60 * 60 * 1000;

function getState() {
  const state = JSON.parse(readState.get().data);
  if (!Array.isArray(state.purchases)) state.purchases = [];
  return state;
}

function storeState(state) {
  writeState.run(JSON.stringify(state));
}

function inTransaction(action) {
  database.exec('BEGIN IMMEDIATE');
  try {
    const result = action();
    database.exec('COMMIT');
    return result;
  } catch (error) {
    database.exec('ROLLBACK');
    throw error;
  }
}

function send(response, status, value) {
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(value));
}

function fail(status, message) {
  const error = new Error(message);
  error.status = status;
  throw error;
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    let body = '';
    request.setEncoding('utf8');
    request.on('data', chunk => {
      body += chunk;
      if (body.length > 2_000_000) {
        reject(Object.assign(new Error('Request is too large'), { status: 413 }));
        request.destroy();
      }
    });
    request.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        reject(Object.assign(new Error('Request body must be valid JSON'), { status: 400 }));
      }
    });
    request.on('error', reject);
  });
}

function cleanText(value, label, maxLength = 200) {
  if (typeof value !== 'string' || !value.trim() || value.trim().length > maxLength) {
    fail(400, `${label} is required and must be at most ${maxLength} characters`);
  }
  return value.trim();
}

function makePassword(password) {
  const salt = randomBytes(16).toString('hex');
  const hash = scryptSync(password, salt, 64).toString('hex');
  return { salt, hash };
}

function passwordMatches(password, salt, hash) {
  const actual = scryptSync(password, salt, 64);
  const expected = Buffer.from(hash, 'hex');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

function requireSession(request, role) {
  const token = (request.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const session = sessions.get(token);
  if (!session || session.expiresAt <= Date.now()) {
    if (token) sessions.delete(token);
    fail(401, 'Please sign in again');
  }
  if (role && session.role !== role) fail(403, 'You do not have permission for this action');
  return { token, ...session };
}

function profileFor(account) {
  return { id: account.id, username: account.username, name: account.display_name, role: account.role };
}

function startSession(response, account) {
  const token = randomBytes(32).toString('hex');
  const profile = profileFor(account);
  sessions.set(token, { ...profile, expiresAt: Date.now() + sessionLifetimeMs });
  send(response, 200, { token, user: profile });
}

function customersFromAccounts() {
  return database.prepare("SELECT id, username, display_name, created_at FROM accounts WHERE role = 'customer' ORDER BY created_at DESC")
    .all().map(row => ({ id: row.id, u: row.username, name: row.display_name, created: row.created_at }));
}

function publicCatalog() {
  const state = getState();
  return {
    products: state.products.map(({ cost, wholesale, ...product }) => product),
    cats: state.cats,
    subs: state.subs,
    settings: { firm: state.settings.firm, free: state.settings.free }
  };
}

function validateStateInput(body) {
  for (const key of ['products', 'cats', 'subs', 'partners', 'orders', 'expenses', 'purchases']) {
    if (!Array.isArray(body[key])) fail(400, `Invalid ${key} data`);
  }
  if (body.products.length > 5000 || body.orders.length > 50000 || body.expenses.length > 50000) {
    fail(400, 'The submitted data exceeds the allowed size');
  }
}

function persistAdminState(input) {
  return inTransaction(() => {
  const current = getState();
  const oldProducts = new Map(current.products.map(product => [String(product.id), product]));
  const products = input.products.map(product => {
    if (!product || !Number.isFinite(Number(product.id)) || !String(product.n || '').trim()) {
      fail(400, 'Every product needs a valid id and name');
    }
    const previous = oldProducts.get(String(product.id));
    return {
      ...product,
      p: Math.max(0, Number(product.p) || 0),
      cost: Math.max(0, Number(product.cost) || 0),
      stock: previous ? Math.max(0, Number(previous.stock) || 0) : Math.max(0, Number(product.stock) || 0)
    };
  });

  const incomingOrders = new Map(input.orders.filter(order => order && order.id).map(order => [String(order.id), order]));
  const orders = current.orders.map(order => {
    const incoming = incomingOrders.get(String(order.id));
    return incoming ? { ...order, ...incoming, items: order.items } : order;
  });
  const existingOrderIds = new Set(orders.map(order => String(order.id)));
  input.orders.forEach(order => {
    if (order && order.id && !existingOrderIds.has(String(order.id))) orders.push(order);
  });

  const accounts = customersFromAccounts();
  const legacyCustomers = Array.isArray(current.customers) ? current.customers : [];
  const customerById = new Map([...legacyCustomers, ...accounts].map(customer => [customer.id || customer.u, customer]));
  const settings = input.settings && typeof input.settings === 'object'
    ? { ...current.settings, ...input.settings }
    : current.settings;
  const next = {
    ...current,
    products,
    cats: input.cats,
    subs: input.subs,
    partners: input.partners,
    orders,
    expenses: input.expenses,
    purchases: input.purchases,
    customers: [...customerById.values()],
    settings
  };
  storeState(next);
  return next;
  });
}

function importLegacyAdminState(input) {
  return inTransaction(() => {
    const current = getState();
    const importedProducts = input.products.map(product => {
      if (!product || !Number.isFinite(Number(product.id)) || !String(product.n || '').trim()) {
        fail(400, 'Legacy backup contains an invalid product');
      }
      return {
        ...product,
        p: Math.max(0, Number(product.p) || 0),
        cost: Math.max(0, Number(product.cost) || 0),
        stock: Math.max(0, Number(product.stock) || 0)
      };
    });
    const orders = new Map((input.orders || []).filter(order => order?.id).map(order => [String(order.id), order]));
    current.orders.forEach(order => orders.set(String(order.id), order));
    const expenses = new Map((input.expenses || []).map(expense => [JSON.stringify(expense), expense]));
    current.expenses.forEach(expense => expenses.set(JSON.stringify(expense), expense));
    const customerMap = new Map([...(input.customers || []), ...customersFromAccounts(), ...current.customers]
      .map(customer => [customer.id || customer.u, customer]));
    const state = {
      ...current,
      products: importedProducts,
      cats: input.cats,
      subs: input.subs,
      partners: input.partners,
      orders: [...orders.values()],
      expenses: [...expenses.values()],
      purchases: [...(input.purchases || []), ...current.purchases],
      customers: [...customerMap.values()],
      settings: input.settings && typeof input.settings === 'object' ? input.settings : current.settings
    };
    storeState(state);
    return state;
  });
}

function servePage(request, response, pathname) {
  if (request.method !== 'GET' && request.method !== 'HEAD') return send(response, 405, { error: 'Method not allowed' });
  const requested = pathname === '/' ? 'MGR_LIFE_STYLE_v22_STORE.html' : decodeURIComponent(pathname.slice(1));
  const allowed = new Set(['MGR_LIFE_STYLE_v22_STORE.html', 'MGR_LIFE_STYLE_v22_ADMIN.html', 'accounting.html']);
  const file = normalize(requested);
  if (!allowed.has(file) || file.includes('..')) return send(response, 404, { error: 'Not found' });
  const fullPath = join(root, file);
  if (!existsSync(fullPath)) return send(response, 404, { error: 'Not found' });
  response.writeHead(200, {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'no-cache',
    'X-Content-Type-Options': 'nosniff'
  });
  if (request.method === 'HEAD') return response.end();
  createReadStream(fullPath).pipe(response);
}

async function handle(request, response) {
  const url = new URL(request.url, 'http://127.0.0.1');
  const { pathname } = url;
  if (!pathname.startsWith('/api/')) return servePage(request, response, pathname);
  if (request.method === 'GET' && pathname === '/api/health') return send(response, 200, { ok: true, localOnly: true });
  if (request.method === 'GET' && pathname === '/api/setup-status') {
    const row = database.prepare("SELECT 1 AS exists_flag FROM accounts WHERE role = 'admin' LIMIT 1").get();
    return send(response, 200, { adminExists: Boolean(row) });
  }
  if (request.method === 'POST' && pathname === '/api/admin/setup') {
    const body = await readBody(request);
    const username = cleanText(body.username, 'Admin username', 120).toLowerCase();
    const name = cleanText(body.name, 'Admin name');
    if (typeof body.password !== 'string' || body.password.length < 6 || body.password.length > 200) {
      fail(400, 'Admin password must contain at least 6 characters');
    }
    const { salt, hash } = makePassword(body.password);
    const account = inTransaction(() => {
      if (database.prepare("SELECT 1 FROM accounts WHERE role = 'admin' LIMIT 1").get()) fail(409, 'Admin setup is already complete');
      database.prepare('INSERT INTO accounts (id, username, display_name, role, password_salt, password_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)')
        .run(randomUUID(), username, name, 'admin', salt, hash, new Date().toISOString());
      return database.prepare("SELECT * FROM accounts WHERE username = ?").get(username);
    });
    return startSession(response, account);
  }
  if (request.method === 'POST' && pathname === '/api/auth/login') {
    const body = await readBody(request);
    const username = cleanText(body.username, 'Username', 120).toLowerCase();
    const role = body.role === 'admin' ? 'admin' : 'customer';
    const key = `${role}:${username}`;
    const attempts = loginFailures.get(key) || { count: 0, until: 0 };
    if (attempts.until > Date.now()) fail(429, 'Too many login attempts. Try again in 15 minutes');
    const account = database.prepare('SELECT * FROM accounts WHERE username = ? AND role = ?').get(username, role);
    if (typeof body.password !== 'string' || !account || !passwordMatches(body.password, account.password_salt, account.password_hash)) {
      attempts.count++;
      if (attempts.count >= 8) attempts.until = Date.now() + 15 * 60 * 1000;
      loginFailures.set(key, attempts);
      fail(401, 'Username or password is incorrect');
    }
    loginFailures.delete(key);
    return startSession(response, account);
  }
  if (request.method === 'POST' && pathname === '/api/customer/signup') {
    const body = await readBody(request);
    const name = cleanText(body.name, 'Name');
    const username = cleanText(body.email, 'Email', 254).toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(username)) fail(400, 'Enter a valid email address');
    if (typeof body.password !== 'string' || body.password.length < 8 || body.password.length > 200) {
      fail(400, 'Password must contain at least 8 characters');
    }
    const { salt, hash } = makePassword(body.password);
    const id = randomUUID();
    try {
      database.prepare('INSERT INTO accounts (id, username, display_name, role, password_salt, password_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)')
        .run(id, username, name, 'customer', salt, hash, new Date().toISOString());
    } catch (error) {
      if (String(error.message).includes('UNIQUE')) fail(409, 'An account with this email already exists');
      throw error;
    }
    const state = getState();
    state.customers.unshift({ id, u: username, name, created: new Date().toISOString() });
    storeState(state);
    const account = database.prepare('SELECT * FROM accounts WHERE id = ?').get(id);
    return startSession(response, account);
  }
  if (request.method === 'GET' && pathname === '/api/catalog') return send(response, 200, publicCatalog());
  if (request.method === 'GET' && pathname === '/api/admin/state') {
    requireSession(request, 'admin');
    const state = getState();
    state.customers = customersFromAccounts().concat(state.customers.filter(customer => !customer.id));
    return send(response, 200, state);
  }
  if (request.method === 'GET' && pathname === '/api/accounting/state') {
    requireSession(request, 'admin');
    const state = getState();
    state.customers = customersFromAccounts().concat(state.customers.filter(customer => !customer.id));
    return send(response, 200, state);
  }
  if (request.method === 'POST' && pathname === '/api/accounting/purchases') {
    requireSession(request, 'admin');
    const body = await readBody(request);
    const supplier = cleanText(body.supplier, 'Supplier name');
    const item = cleanText(body.item, 'Purchase description');
    const amount = Number(body.amount);
    if (!Number.isFinite(amount) || amount <= 0) fail(400, 'Purchase amount must be greater than zero');
    const date = body.date ? new Date(body.date) : new Date();
    if (Number.isNaN(date.getTime())) fail(400, 'Enter a valid purchase date');
    const quantity = body.quantity === '' || body.quantity == null ? null : Number(body.quantity);
    if (quantity !== null && (!Number.isInteger(quantity) || quantity <= 0)) fail(400, 'Quantity must be a positive whole number');
    const purchase = inTransaction(() => {
      const state = getState();
      const product = body.productId ? state.products.find(entry => String(entry.id) === String(body.productId)) : null;
      if (body.productId && !product) fail(404, 'Selected product was not found');
      if (product && !quantity) fail(400, 'Enter the quantity to add to product stock');
      if (product) product.stock = Math.max(0, Number(product.stock) || 0) + quantity;
      const entry = {
        id: `PUR-${Date.now().toString(36).toUpperCase()}-${randomBytes(2).toString('hex').toUpperCase()}`,
        supplier,
        invoice: typeof body.invoice === 'string' ? body.invoice.trim().slice(0, 100) : '',
        item,
        productId: product ? product.id : null,
        quantity,
        amount,
        date: date.toISOString(),
        notes: typeof body.notes === 'string' ? body.notes.trim().slice(0, 1000) : ''
      };
      state.purchases.unshift(entry);
      storeState(state);
      return entry;
    });
    return send(response, 201, purchase);
  }
  if (request.method === 'POST' && pathname === '/api/accounting/expenses') {
    requireSession(request, 'admin');
    const body = await readBody(request);
    const title = cleanText(body.title, 'Expense description');
    const amount = Number(body.amount);
    if (!Number.isFinite(amount) || amount <= 0) fail(400, 'Expense amount must be greater than zero');
    const date = body.date ? new Date(body.date) : new Date();
    if (Number.isNaN(date.getTime())) fail(400, 'Enter a valid expense date');
    const expense = inTransaction(() => {
      const state = getState();
      const entry = {
        id: randomUUID(),
        title,
        category: typeof body.category === 'string' ? body.category.trim().slice(0, 100) : '',
        amount,
        date: date.toISOString(),
        notes: typeof body.notes === 'string' ? body.notes.trim().slice(0, 1000) : ''
      };
      state.expenses.unshift(entry);
      storeState(state);
      return entry;
    });
    return send(response, 201, expense);
  }
  if (request.method === 'PUT' && pathname === '/api/admin/state') {
    requireSession(request, 'admin');
    const body = await readBody(request);
    validateStateInput(body);
    return send(response, 200, persistAdminState(body));
  }
  if (request.method === 'POST' && pathname === '/api/admin/import') {
    requireSession(request, 'admin');
    const body = await readBody(request);
    validateStateInput(body);
    return send(response, 200, importLegacyAdminState(body));
  }
  if (request.method === 'POST' && pathname === '/api/admin/password') {
    const session = requireSession(request, 'admin');
    const body = await readBody(request);
    if (typeof body.password !== 'string' || body.password.length < 6 || body.password.length > 200) {
      fail(400, 'New password must contain at least 6 characters');
    }
    const account = database.prepare('SELECT * FROM accounts WHERE id = ?').get(session.id);
    if (!account || typeof body.currentPassword !== 'string' || !passwordMatches(body.currentPassword, account.password_salt, account.password_hash)) {
      fail(401, 'Current password is incorrect');
    }
    const { salt, hash } = makePassword(body.password);
    database.prepare('UPDATE accounts SET password_salt = ?, password_hash = ? WHERE id = ?').run(salt, hash, session.id);
    return send(response, 200, { ok: true });
  }
  if (request.method === 'GET' && pathname === '/api/customer/orders') {
    const session = requireSession(request, 'customer');
    const state = getState();
    return send(response, 200, state.orders.filter(order => order.customerId === session.id));
  }
  if (request.method === 'POST' && pathname === '/api/orders') {
    const session = requireSession(request, 'customer');
    const body = await readBody(request);
    if (!Array.isArray(body.items) || body.items.length < 1 || body.items.length > 100) fail(400, 'Cart is empty or invalid');
    const phone = typeof body.phone === 'string' ? body.phone.trim().slice(0, 40) : '';
    const address = cleanText(body.address, 'Shipping address', 1000);
    if (address.length < 8) fail(400, 'Shipping address is too short');
    const order = inTransaction(() => {
      const state = getState();
      const account = database.prepare('SELECT id, username, display_name FROM accounts WHERE id = ?').get(session.id);
      if (!account) fail(401, 'Please sign in again');
      const requested = new Map();
      body.items.forEach(item => {
        const id = String(item?.id ?? '');
        const qty = Number(item?.qty);
        if (!id || !Number.isInteger(qty) || qty < 1 || qty > 100) fail(400, 'Invalid product quantity');
        requested.set(id, (requested.get(id) || 0) + qty);
      });
      let total = 0;
      let cost = 0;
      const items = [];
      for (const [id, qty] of requested) {
        const product = state.products.find(entry => String(entry.id) === id);
        if (!product) fail(404, 'A cart product is no longer available');
        if (Number(product.stock) < qty) fail(409, `${product.n} does not have enough stock`);
        total += Number(product.p) * qty;
        cost += Number(product.cost) * qty;
        items.push({ id: product.id, qty, price: Number(product.p) });
        product.stock -= qty;
      }
      const order = {
        id: `MGR-${Date.now().toString(36).toUpperCase()}-${randomBytes(2).toString('hex').toUpperCase()}`,
        customerId: account.id,
        customer: account.display_name,
        email: account.username,
        phone,
        address,
        total,
        status: 'Pending Payment',
        items,
        created: new Date().toISOString(),
        paid: false,
        netProfit: 0
      };
      state.orders.unshift(order);
      storeState(state);
      return order;
    });
    return send(response, 201, order);
  }
  if (request.method === 'POST' && pathname === '/api/auth/logout') {
    const session = requireSession(request);
    sessions.delete(session.token);
    return send(response, 200, { ok: true });
  }
  if (request.method === 'GET' && pathname === '/api/session') {
    const session = requireSession(request);
    return send(response, 200, { user: { id: session.id, username: session.username, name: session.name, role: session.role } });
  }
  return send(response, 404, { error: 'API route not found' });
}

const server = createServer((request, response) => {
  handle(request, response).catch(error => {
    if (response.headersSent || response.destroyed) return;
    const status = Number(error.status) || 500;
    if (status >= 500) console.error(error);
    send(response, status, { error: status === 500 ? 'Local backend error' : error.message });
  });
});

const port = Number(process.env.PORT) || 8765;
server.listen(port, '127.0.0.1', () => {
  console.log(`MGR LIFE STYLE local backend: http://127.0.0.1:${port}`);
  console.log(`Store: http://127.0.0.1:${port}/MGR_LIFE_STYLE_v22_STORE.html`);
  console.log(`Admin: http://127.0.0.1:${port}/MGR_LIFE_STYLE_v22_ADMIN.html`);
  console.log(`Database: ${join(dataDirectory, 'mgr-life.sqlite')}`);
});

function shutdown() {
  server.close(() => {
    database.close();
    process.exit(0);
  });
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
