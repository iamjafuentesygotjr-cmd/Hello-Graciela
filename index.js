// HelloGraciela lead generation server (combined file). Vercel runs this for every /api request.

// server/app.js
import express from "express";
import multer from "multer";
import path4 from "node:path";

// server/db.js
import { AsyncLocalStorage } from "node:async_hooks";
import path from "node:path";
var txStore = new AsyncLocalStorage();
var driver = null;
var DB_URL = process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.DATABASE_URL_UNPOOLED || "";
async function connect() {
  if (DB_URL) {
    const { default: pg } = await import("pg");
    const local = /localhost|127\.0\.0\.1/.test(DB_URL);
    const pool = new pg.Pool({
      connectionString: DB_URL,
      max: Number(process.env.PG_POOL_MAX || 5),
      idleTimeoutMillis: 1e4,
      ssl: local || /sslmode=disable/.test(DB_URL) ? false : { rejectUnauthorized: false }
    });
    return {
      query: (text, params) => pool.query(text, params),
      async transaction(fn) {
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          const r = await fn((t, p) => client.query(t, p));
          await client.query("COMMIT");
          return r;
        } catch (e) {
          await client.query("ROLLBACK").catch(() => {
          });
          throw e;
        } finally {
          client.release();
        }
      }
    };
  }
  if (process.env.VERCEL) throw new Error("DATABASE_URL is not set. Add a Postgres database (for example Neon) to this Vercel project.");
  const { PGlite } = await import("@electric-sql/pglite");
  const dir = process.env.PGLITE_DIR || path.resolve(process.env.DATA_DIR || "data", "pglite");
  const pgl = new PGlite(dir);
  await pgl.waitReady;
  return {
    query: (text, params) => pgl.query(text, params),
    transaction: (fn) => pgl.transaction((tx2) => fn((t, p) => tx2.query(t, p)))
  };
}
function toPg(sql) {
  let i = 0;
  return sql.replace(/\?/g, () => `$${++i}`);
}
var ready = null;
function init() {
  if (!ready) ready = (async () => {
    driver = await connect();
    await migrate();
  })().catch((e) => {
    ready = null;
    throw e;
  });
  return ready;
}
async function raw(sql, params = []) {
  await init();
  const q = txStore.getStore();
  const clean = params.map((v) => v === void 0 ? null : v);
  const res = q ? await q(toPg(sql), clean) : await driver.query(toPg(sql), clean);
  return res;
}
var all = async (sql, params) => (await raw(sql, params)).rows;
var get = async (sql, params) => (await raw(sql, params)).rows[0];
async function run(sql, params) {
  const r = await raw(sql, params);
  return { changes: r.rowCount ?? r.affectedRows ?? 0, rows: r.rows };
}
async function tx(fn) {
  await init();
  if (txStore.getStore()) return fn();
  return driver.transaction((q) => txStore.run(q, fn));
}
var now = () => (/* @__PURE__ */ new Date()).toISOString();
async function getSetting(key, fallback) {
  const row = await get("SELECT value FROM settings WHERE key = ?", [key]);
  return row ? JSON.parse(row.value) : fallback;
}
async function setSetting(key, value) {
  await run("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", [key, JSON.stringify(value)]);
}
var JSON_COLS = ["sources", "gaps", "extra"];
function hydrateLead(row) {
  if (!row) return row;
  const out = { ...row };
  for (const c of JSON_COLS) out[c] = row[c] ? JSON.parse(row[c]) : c === "gaps" ? {} : null;
  return out;
}
async function migrate() {
  const statements = `
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  username TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('owner','assistant')),
  password_hash TEXT NOT NULL,
  must_change INTEGER NOT NULL DEFAULT 1,
  active INTEGER NOT NULL DEFAULT 1,
  targets TEXT,
  created_at TEXT NOT NULL,
  password_changed_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower ON users (lower(username));
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS leads (
  id SERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  title TEXT,
  linkedin_url TEXT NOT NULL,
  linkedin_key TEXT NOT NULL UNIQUE,
  company TEXT, website TEXT, city TEXT, country TEXT, market TEXT, location_basis TEXT,
  brand_category TEXT, fit TEXT, fit_basis TEXT, priority TEXT,
  assigned_to INTEGER REFERENCES users(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'New',
  status_updated_at TEXT, status_updated_by INTEGER,
  profile_opened_at TEXT, profile_opened_by INTEGER,
  follow_up_due_at TEXT, overdue_notified_at TEXT, meeting_at TEXT,
  connection_note TEXT, followup_note TEXT,
  company_size TEXT, business_email TEXT, email_type TEXT, email_source TEXT,
  qualification_notes TEXT, fit_rationale TEXT, verification_notes TEXT, research_date TEXT,
  sources TEXT, gaps TEXT, extra TEXT,
  import_id INTEGER, created_by INTEGER,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS leads_assigned ON leads(assigned_to);
CREATE INDEX IF NOT EXISTS leads_status ON leads(status);
CREATE TABLE IF NOT EXISTS activity (
  id SERIAL PRIMARY KEY,
  lead_id INTEGER REFERENCES leads(id) ON DELETE CASCADE,
  user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
  type TEXT NOT NULL,
  from_status TEXT, to_status TEXT, detail TEXT,
  voided INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS activity_lead ON activity(lead_id);
CREATE INDEX IF NOT EXISTS activity_created ON activity(created_at);
CREATE TABLE IF NOT EXISTS notifications (
  id SERIAL PRIMARY KEY,
  recipient_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type TEXT NOT NULL, title TEXT NOT NULL, body TEXT,
  lead_id INTEGER, import_id INTEGER, dedupe_key TEXT,
  created_at TEXT NOT NULL, read_at TEXT,
  UNIQUE (recipient_id, dedupe_key)
);
CREATE TABLE IF NOT EXISTS imports (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
  filename TEXT NOT NULL, file_kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'preview',
  parsed TEXT, summary TEXT,
  created_at TEXT NOT NULL, completed_at TEXT
);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS changes (id SERIAL PRIMARY KEY, scopes TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS login_failures (key TEXT PRIMARY KEY, first_at BIGINT NOT NULL, count INTEGER NOT NULL);
`;
  for (const s of statements.split(";").map((x) => x.trim()).filter(Boolean)) {
    try {
      await driver.query(s, []);
    } catch (e) {
      if (!/already exists|duplicate key/i.test(e.message)) throw e;
    }
  }
}

// server/auth.js
import crypto from "node:crypto";
var SCRYPT = { N: 16384, r: 8, p: 1, keylen: 64 };
var SESSION_DAYS = 14;
var COOKIE = "obs_sid";
function hashPassword(password) {
  const salt = crypto.randomBytes(16);
  const hash = crypto.scryptSync(password, salt, SCRYPT.keylen, { N: SCRYPT.N, r: SCRYPT.r, p: SCRYPT.p });
  return `scrypt$${SCRYPT.N}$${SCRYPT.r}$${SCRYPT.p}$${salt.toString("base64")}$${hash.toString("base64")}`;
}
function verifyPassword(password, stored) {
  try {
    const [alg, N, r, p, saltB64, hashB64] = stored.split("$");
    if (alg !== "scrypt") return false;
    const expected = Buffer.from(hashB64, "base64");
    const actual = crypto.scryptSync(password, Buffer.from(saltB64, "base64"), expected.length, { N: +N, r: +r, p: +p });
    return crypto.timingSafeEqual(expected, actual);
  } catch {
    return false;
  }
}
var DUMMY_HASH = hashPassword(crypto.randomBytes(12).toString("hex"));
async function checkCredentials(username, password) {
  const user = await get("SELECT * FROM users WHERE lower(username) = lower(?) AND active = 1", [String(username || "").trim()]);
  const ok = verifyPassword(String(password || ""), user ? user.password_hash : DUMMY_HASH);
  return ok && user ? user : null;
}
var sha = (t) => crypto.createHash("sha256").update(t).digest("hex");
async function createSession(userId) {
  const token = crypto.randomBytes(32).toString("base64url");
  const expires = new Date(Date.now() + SESSION_DAYS * 864e5).toISOString();
  await run("INSERT INTO sessions (id, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)", [sha(token), userId, now(), expires]);
  await run("DELETE FROM sessions WHERE expires_at < ?", [now()]);
  return { token, expires };
}
async function destroySession(token) {
  if (token) await run("DELETE FROM sessions WHERE id = ?", [sha(token)]);
}
async function destroyUserSessions(userId, exceptToken) {
  if (exceptToken) await run("DELETE FROM sessions WHERE user_id = ? AND id <> ?", [userId, sha(exceptToken)]);
  else await run("DELETE FROM sessions WHERE user_id = ?", [userId]);
}
async function sessionUser(token) {
  if (!token) return null;
  const row = await get(`SELECT u.*, s.expires_at FROM sessions s JOIN users u ON u.id = s.user_id
    WHERE s.id = ? AND u.active = 1`, [sha(token)]);
  if (!row) return null;
  if (row.expires_at < now()) {
    await destroySession(token);
    return null;
  }
  return row;
}
function parseCookies(header = "") {
  const out = {};
  for (const part of header.split(";")) {
    const i = part.indexOf("=");
    if (i > 0) out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim());
  }
  return out;
}
var WINDOW = 15 * 60 * 1e3;
var MAX = 8;
async function tooManyAttempts(keys) {
  const t = Date.now();
  for (const k of keys) {
    const f = await get("SELECT first_at, count FROM login_failures WHERE key = ?", [k]);
    if (f && t - Number(f.first_at) <= WINDOW && Number(f.count) >= MAX) return true;
  }
  return false;
}
async function recordFailure(keys) {
  const t = Date.now();
  for (const k of keys) {
    await run(
      `INSERT INTO login_failures (key, first_at, count) VALUES (?, ?, 1)
      ON CONFLICT (key) DO UPDATE SET
        count = CASE WHEN ? - login_failures.first_at > ? THEN 1 ELSE login_failures.count + 1 END,
        first_at = CASE WHEN ? - login_failures.first_at > ? THEN ? ELSE login_failures.first_at END`,
      [k, t, t, WINDOW, t, WINDOW, t]
    );
  }
}
async function clearFailures(keys) {
  for (const k of keys) await run("DELETE FROM login_failures WHERE key = ?", [k]);
}
function publicUser(u) {
  if (!u) return null;
  return {
    id: u.id,
    username: u.username,
    display_name: u.display_name,
    role: u.role,
    must_change: !!u.must_change,
    active: !!u.active
  };
}
function validatePassword(pw) {
  if (typeof pw !== "string" || pw.length < 10) return "Use at least 10 characters.";
  if (pw.length > 200) return "That password is too long.";
  if (!/[A-Za-z]/.test(pw) || !/[0-9]/.test(pw)) return "Include at least one letter and one number.";
  return null;
}
function validateUsername(name) {
  if (typeof name !== "string" || !/^[A-Za-z0-9._-]{3,40}$/.test(name.trim())) return "Usernames are 3\u201340 characters: letters, numbers, dots, dashes or underscores.";
  return null;
}

// server/domain.js
var STATUSES = [
  "New",
  "Assigned",
  "Profile opened",
  "Connection sent",
  "Connected",
  "Replied",
  "Follow-up due",
  "Meeting booked",
  "Not a fit"
];
var FITS = ["Primary fit", "Secondary fit", "Not a fit"];
var PRIORITIES = ["High", "Medium", "Low"];
var METRICS = [
  { key: "profiles_opened", label: "Profiles opened", short: "Opened" },
  { key: "connections_sent", label: "Connection requests sent", short: "Requests" },
  { key: "connections_accepted", label: "Connections accepted", short: "Accepted" },
  { key: "replies", label: "Replies", short: "Replies" },
  { key: "followups", label: "Follow-ups sent", short: "Follow-ups" },
  { key: "meetings", label: "Meetings booked", short: "Meetings" }
];
var STATUS_METRIC = {
  "Connection sent": "connections_sent",
  "Connected": "connections_accepted",
  "Replied": "replies",
  "Meeting booked": "meetings"
};
var DEFAULT_TARGETS = {
  profiles_opened: 30,
  connections_sent: 20,
  connections_accepted: 8,
  replies: 3,
  followups: 10,
  meetings: 1
};
var CORE_CATEGORY = /(skin|spf|sun ?care|supplement|vitamin|collagen|ingestible|nutri|probiotic|gut|greens|serum|derma)/i;
var ADJACENT_CATEGORY = /(beauty|wellness|cosmetic|hair|body ?care|self[- ]care|clean|makeup|fragrance|personal care|health)/i;
var CORE_MARKET = /(united states|^us$|^usa$|u\.s\.|america|australia|^au$|^aus$)/i;
function normaliseFit(value) {
  if (!value) return null;
  const v = String(value).toLowerCase();
  if (/not\s*(a\s*)?fit|disqualif|exclude|^no$/.test(v)) return "Not a fit";
  if (/primary|tier\s*1|^a$|high/.test(v)) return "Primary fit";
  if (/secondary|tier\s*2|^b$|medium/.test(v)) return "Secondary fit";
  return null;
}
function suggestFit(lead) {
  const cat = lead.brand_category || "";
  const market = [lead.country, lead.market].filter(Boolean).join(" ");
  const coreCat = CORE_CATEGORY.test(cat);
  const adjCat = !coreCat && ADJACENT_CATEGORY.test(cat);
  const coreMarket = market.split(/[\/,]/).some((m) => CORE_MARKET.test(m.trim()));
  const founder = /(founder|owner|ceo|chief executive)/i.test(lead.title || "");
  if (!cat && !market) return { fit: null, reason: "Not enough information to classify" };
  if (coreCat && coreMarket) return { fit: "Primary fit", reason: founder ? "Core category, US/Australia market, founder-level contact" : "Core category and US/Australia market" };
  if (coreCat && !coreMarket || adjCat && coreMarket) return { fit: "Secondary fit", reason: coreCat ? "Core category outside the US/Australia focus" : "Adjacent beauty or wellness category" };
  if (cat && !coreCat && !adjCat) return { fit: "Not a fit", reason: "Category is outside skincare, supplements and ingestible beauty" };
  return { fit: null, reason: "Needs review" };
}
var DEFAULT_TEMPLATES = {
  connection: "Hi {first_name}, I came across {company} and liked your focus on {focus}. I'm with HelloGraciela, a digital studio that helps founder-led {sector} brands explain their products through organic and creator content. I'd be glad to connect.",
  followup: "Thanks for connecting, {first_name}. We work with {sector} brands on content that explains ingredients and routines clearly, without the hype. If it's ever useful, I'd be happy to share a few ideas for {company}. No pressure either way."
};
function categoryPhrases(category = "") {
  const c = category.toLowerCase();
  if (/spf|sun/.test(c)) return { focus: "sun care education", sector: "skincare" };
  if (/derma/.test(c)) return { focus: "dermatologist-informed routines", sector: "skincare" };
  if (/ingredient/.test(c)) return { focus: "ingredient-led skincare", sector: "skincare" };
  if (/supplement|vitamin|functional/.test(c)) return { focus: "supplement education", sector: "supplement and wellness" };
  if (/ingestible|collagen/.test(c)) return { focus: "ingestible beauty", sector: "ingestible beauty" };
  if (/skin/.test(c)) return { focus: "thoughtful skincare", sector: "skincare" };
  return { focus: "your products", sector: "beauty and wellness" };
}
function firstName(fullName = "") {
  return String(fullName).trim().split(/\s+/)[0] || "there";
}
function renderTemplate(template, lead) {
  const { focus, sector } = categoryPhrases(lead.brand_category || "");
  const vars = {
    first_name: firstName(lead.full_name),
    full_name: lead.full_name || "",
    company: lead.company || "your brand",
    title: lead.title || "",
    focus,
    sector
  };
  return template.replace(/\{(\w+)\}/g, (m, k) => k in vars ? vars[k] : m);
}
function nextAction(lead) {
  switch (lead.status) {
    case "New":
    case "Assigned":
      return "Open the LinkedIn profile and check the fit";
    case "Profile opened":
      return "Send a connection request with the note, then mark it as sent";
    case "Connection sent":
      return "Wait for acceptance, then mark as connected";
    case "Connected":
      return "Send the follow-up message and log it";
    case "Replied":
      return "Reply personally and suggest a short call";
    case "Follow-up due":
      return lead.follow_up_due_at ? "Follow up by the due date" : "Set a follow-up date";
    case "Meeting booked":
      return "Share call details with the owner";
    case "Not a fit":
      return "No further action";
    default:
      return "";
  }
}
var LI_RE = /^(?:https?:\/\/)?(?:([a-z]{2,3})\.)?linkedin\.com\/(in|pub)\/([^\/?#\s]+)\/?(?:[?#].*)?$/i;
function parseLinkedIn(raw2) {
  if (!raw2) return { ok: false, error: "LinkedIn URL is missing" };
  const s = String(raw2).trim();
  if (/linkedin\.com\/(company|school|showcase)\//i.test(s)) return { ok: false, error: "This is a company page, not a personal profile" };
  if (/linkedin\.com\/(sales|talent|recruiter)\//i.test(s)) return { ok: false, error: "Sales Navigator or Recruiter links need the public /in/ profile URL" };
  const m = s.match(LI_RE);
  if (!m) return { ok: false, error: "Not a valid LinkedIn profile URL" };
  let slug;
  try {
    slug = decodeURIComponent(m[3]);
  } catch {
    slug = m[3];
  }
  const sub = (m[1] || "www").toLowerCase();
  const url = `https://${sub}.linkedin.com/${m[2].toLowerCase()}/${encodeURIComponent(slug).replace(/%2D/gi, "-")}`;
  return { ok: true, url, key: `${m[2].toLowerCase()}/${slug.toLowerCase()}` };
}
var EMAIL_RE = /[A-Z0-9._%+'-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i;

// server/services.js
async function broadcast(...scopes) {
  await run("INSERT INTO changes (scopes, created_at) VALUES (?, ?)", [scopes.join(","), now()]);
}
async function changesSince(after) {
  const latest = (await get("SELECT COALESCE(MAX(id), 0)::int AS id FROM changes")).id;
  if (after === null || after === void 0 || Number.isNaN(after)) return { latest, scopes: [] };
  if (after > latest) return { latest, scopes: ["*"] };
  const rows = await all("SELECT scopes FROM changes WHERE id > ? ORDER BY id LIMIT 500", [after]);
  const scopes = [...new Set(rows.flatMap((r) => r.scopes.split(",")))];
  return { latest, scopes };
}
var ownerIds = async () => (await all("SELECT id FROM users WHERE role = 'owner' AND active = 1")).map((r) => r.id);
async function userNames() {
  const m = {};
  for (const u of await all("SELECT id, display_name FROM users")) m[u.id] = u.display_name;
  return m;
}
function targetsFor(user, defaults = {}) {
  const t = user && user.targets ? JSON.parse(user.targets) : null;
  return { ...DEFAULT_TARGETS, ...defaults, ...t || {} };
}
var defaultTargets = () => getSetting("default_targets", {});
var timezone = () => getSetting("timezone", process.env.APP_TIMEZONE || "Asia/Manila");
function dayKey(iso, tz) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(iso));
}
function addDays(key, n) {
  const [y, m, d] = key.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10);
}
function weekStart(key) {
  const [y, m, d] = key.split("-").map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  return addDays(key, -((dow + 6) % 7));
}
async function fmtInTz(iso) {
  return new Date(iso).toLocaleString("en-US", { timeZone: await timezone(), dateStyle: "medium", timeStyle: "short" });
}
async function notify(recipients, n) {
  let created = 0;
  for (const r of new Set(recipients.filter(Boolean))) {
    const res = await run(
      `INSERT INTO notifications (recipient_id, type, title, body, lead_id, import_id, dedupe_key, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (recipient_id, dedupe_key) DO NOTHING`,
      [r, n.type, n.title, n.body || null, n.lead_id || null, n.import_id || null, n.dedupe_key || null, now()]
    );
    created += res.changes;
  }
  if (created) await broadcast("notifications");
}
function serializeLead(row, names) {
  const l = hydrateLead(row);
  return {
    ...l,
    assigned_name: l.assigned_to ? names[l.assigned_to] || "Unknown" : null,
    status_updated_by_name: l.status_updated_by ? names[l.status_updated_by] || "Unknown" : null,
    profile_opened_by_name: l.profile_opened_by ? names[l.profile_opened_by] || "Unknown" : null,
    next_action: nextAction(l),
    follow_up_overdue: !!(l.status === "Follow-up due" && l.follow_up_due_at && l.follow_up_due_at < now())
  };
}
var getLead = (id) => get("SELECT * FROM leads WHERE id = ?", [id]);
function canAccessLead(user, lead) {
  return !!lead && (user.role === "owner" || lead.assigned_to === user.id);
}
async function logActivity(leadId, userId, type, { from = null, to = null, detail = null } = {}) {
  const r = await get(`INSERT INTO activity (lead_id, user_id, type, from_status, to_status, detail, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id`, [leadId, userId, type, from, to, detail ? JSON.stringify(detail) : null, now()]);
  return r.id;
}
async function leadActivity(leadId) {
  const names = await userNames();
  return (await all("SELECT * FROM activity WHERE lead_id = ? ORDER BY created_at DESC, id DESC", [leadId])).map((a) => ({ ...a, detail: a.detail ? JSON.parse(a.detail) : null, user_name: names[a.user_id] || "System" }));
}
function leadLabel(l) {
  return l.company ? `${l.full_name} (${l.company})` : l.full_name;
}
async function setStatus(leadId, status, user, opts = {}) {
  if (!STATUSES.includes(status)) throw httpError(400, "Choose a valid status.");
  const lead = await getLead(leadId);
  if (!canAccessLead(user, lead)) throw httpError(404, "Lead not found.");
  const t = now();
  let due = null, meeting = null;
  if (status === "Follow-up due") {
    const d = opts.follow_up_due_at ? new Date(opts.follow_up_due_at) : new Date(Date.now() + 3 * 864e5);
    if (isNaN(d)) throw httpError(400, "Choose a valid follow-up date.");
    due = d.toISOString();
  }
  if (status === "Meeting booked" && opts.meeting_at) {
    const d = new Date(opts.meeting_at);
    if (isNaN(d)) throw httpError(400, "Choose a valid meeting date.");
    meeting = d.toISOString();
  }
  if (lead.status === status && status !== "Follow-up due" && status !== "Meeting booked") return { activityId: null };
  const activityId = await tx(async () => {
    await run(`UPDATE leads SET status = ?, status_updated_at = ?, status_updated_by = ?,
      follow_up_due_at = ?, overdue_notified_at = NULL,
      meeting_at = CASE WHEN ?::text = 'Meeting booked' THEN COALESCE(?::text, meeting_at) ELSE meeting_at END,
      updated_at = ? WHERE id = ?`, [status, t, user.id, due, status, meeting, t, leadId]);
    return logActivity(leadId, user.id, "status_change", {
      from: lead.status,
      to: status,
      detail: { follow_up_due_at: due, meeting_at: meeting, reason: opts.reason || null, prev_due_at: lead.follow_up_due_at }
    });
  });
  if (status === "Replied") {
    await notify((await ownerIds()).filter((id) => id !== user.id), {
      type: "reply",
      title: `${lead.full_name} replied`,
      body: `${user.display_name} marked ${leadLabel(lead)} as replied.`,
      lead_id: leadId
    });
  }
  if (status === "Meeting booked") {
    await notify((await ownerIds()).filter((id) => id !== user.id), {
      type: "meeting",
      title: `Meeting booked with ${lead.full_name}`,
      body: `${user.display_name} booked a meeting with ${leadLabel(lead)}${meeting ? ` for ${await fmtInTz(meeting)}` : ""}.`,
      lead_id: leadId
    });
  }
  if (STATUS_METRIC[status]) await checkTargets(user, STATUS_METRIC[status]);
  if (due && due < now()) await checkOverdue(true);
  await broadcast("leads", "activity", "metrics");
  return { activityId };
}
async function recordProfileOpen(leadId, user) {
  const lead = await getLead(leadId);
  if (!canAccessLead(user, lead)) throw httpError(404, "Lead not found.");
  const t = now();
  const advance = ["New", "Assigned"].includes(lead.status);
  const to = advance ? "Profile opened" : lead.status;
  const activityId = await tx(async () => {
    await run(
      `UPDATE leads SET profile_opened_at = ?, profile_opened_by = ?, updated_at = ?, status = ?,
      status_updated_at = CASE WHEN ?::boolean THEN ?::text ELSE status_updated_at END,
      status_updated_by = CASE WHEN ?::boolean THEN ?::int ELSE status_updated_by END WHERE id = ?`,
      [t, user.id, t, to, advance, t, advance, user.id, leadId]
    );
    return logActivity(leadId, user.id, "profile_opened", { from: lead.status, to, detail: { url: lead.linkedin_url } });
  });
  await checkTargets(user, "profiles_opened");
  await broadcast("leads", "activity", "metrics");
  return { activityId, url: lead.linkedin_url, status: to, opened_at: t };
}
async function recordFollowupSent(leadId, user, { next_due_at } = {}) {
  const lead = await getLead(leadId);
  if (!canAccessLead(user, lead)) throw httpError(404, "Lead not found.");
  const t = now();
  let to = lead.status, due = null;
  if (next_due_at) {
    const d = new Date(next_due_at);
    if (isNaN(d)) throw httpError(400, "Choose a valid follow-up date.");
    to = "Follow-up due";
    due = d.toISOString();
  } else if (lead.status === "Follow-up due") {
    const prev = await get(`SELECT from_status FROM activity WHERE lead_id = ? AND type = 'status_change'
      AND to_status = 'Follow-up due' AND voided = 0 ORDER BY id DESC LIMIT 1`, [leadId]);
    to = prev && prev.from_status && prev.from_status !== "Follow-up due" ? prev.from_status : "Connected";
  }
  const activityId = await tx(async () => {
    await run(`UPDATE leads SET status = ?, follow_up_due_at = ?, overdue_notified_at = NULL,
      status_updated_at = ?, status_updated_by = ?, updated_at = ? WHERE id = ?`, [to, due, t, user.id, t, leadId]);
    return logActivity(leadId, user.id, "followup_sent", {
      from: lead.status,
      to,
      detail: { prev_due_at: lead.follow_up_due_at, next_due_at: due }
    });
  });
  await checkTargets(user, "followups");
  await broadcast("leads", "activity", "metrics");
  return { activityId };
}
async function undoActivity(activityId, user) {
  const a = await get("SELECT * FROM activity WHERE id = ?", [activityId]);
  if (!a || a.voided) throw httpError(404, "Nothing to undo.");
  const lead = await getLead(a.lead_id);
  if (!canAccessLead(user, lead)) throw httpError(404, "Nothing to undo.");
  if (!["status_change", "profile_opened", "followup_sent"].includes(a.type)) throw httpError(400, "This update can\u2019t be undone.");
  if (user.role !== "owner" && (a.user_id !== user.id || Date.now() - Date.parse(a.created_at) > 15 * 6e4)) {
    throw httpError(403, "Updates can be undone for 15 minutes by the person who made them.");
  }
  if (lead.status !== a.to_status) throw httpError(409, "This lead has been updated since, so it can\u2019t be undone.");
  const detail = a.detail ? JSON.parse(a.detail) : {};
  const t = now();
  await tx(async () => {
    await run("UPDATE activity SET voided = 1 WHERE id = ?", [a.id]);
    const restoreDue = detail.prev_due_at || null;
    await run(
      `UPDATE leads SET status = ?, status_updated_at = ?, status_updated_by = ?, updated_at = ?,
      follow_up_due_at = CASE WHEN ?::text = 'Follow-up due' THEN COALESCE(?::text, follow_up_due_at) ELSE NULL END WHERE id = ?`,
      [a.from_status || lead.status, t, user.id, t, a.from_status, restoreDue, lead.id]
    );
    if (a.type === "profile_opened") {
      const prev = await get(`SELECT created_at, user_id FROM activity WHERE lead_id = ? AND type = 'profile_opened' AND voided = 0 ORDER BY id DESC LIMIT 1`, [lead.id]);
      await run("UPDATE leads SET profile_opened_at = ?, profile_opened_by = ? WHERE id = ?", [prev?.created_at || null, prev?.user_id || null, lead.id]);
    }
    await logActivity(lead.id, user.id, "undo", { from: a.to_status, to: a.from_status, detail: { undone_type: a.type, undone_id: a.id } });
  });
  await broadcast("leads", "activity", "metrics");
}
async function assignLeads(ids, assistantId, user) {
  let assignee = null;
  if (assistantId) {
    assignee = await get("SELECT * FROM users WHERE id = ? AND role = 'assistant' AND active = 1", [assistantId]);
    if (!assignee) throw httpError(400, "Choose an active assistant.");
  }
  const names = await userNames();
  let changed = 0;
  await tx(async () => {
    for (const id of ids) {
      const lead = await getLead(id);
      if (!lead || lead.assigned_to === (assignee?.id ?? null)) continue;
      let status = lead.status;
      if (assignee && status === "New") status = "Assigned";
      if (!assignee && status === "Assigned") status = "New";
      const t = now();
      const statusChanged = status !== lead.status;
      await run(
        `UPDATE leads SET assigned_to = ?, status = ?, updated_at = ?,
        status_updated_at = CASE WHEN ?::boolean THEN ?::text ELSE status_updated_at END,
        status_updated_by = CASE WHEN ?::boolean THEN ?::int ELSE status_updated_by END WHERE id = ?`,
        [assignee?.id ?? null, status, t, statusChanged, t, statusChanged, user.id, id]
      );
      await logActivity(id, user.id, "assigned", {
        from: lead.status,
        to: status,
        detail: { from_user: lead.assigned_to ? names[lead.assigned_to] : null, to_user: assignee?.display_name || null }
      });
      changed++;
    }
  });
  if (assignee && changed) {
    await notify([assignee.id], {
      type: "assigned",
      title: `${changed} ${changed === 1 ? "lead" : "leads"} assigned to you`,
      body: `${user.display_name} added ${changed === 1 ? "a lead" : `${changed} leads`} to your list.`
    });
  }
  await broadcast("leads", "activity", "metrics");
  return changed;
}
async function metricEvents() {
  const rows = await all(`SELECT lead_id, user_id, type, to_status, created_at FROM activity
    WHERE voided = 0 AND type IN ('profile_opened', 'status_change', 'followup_sent') ORDER BY created_at, id`);
  const seen = /* @__PURE__ */ new Set();
  const events = [];
  for (const r of rows) {
    const metric = r.type === "profile_opened" ? "profiles_opened" : r.type === "followup_sent" ? "followups" : STATUS_METRIC[r.to_status];
    if (!metric) continue;
    if (metric !== "followups") {
      const k = `${metric}:${r.lead_id}`;
      if (seen.has(k)) continue;
      seen.add(k);
    }
    events.push({ metric, user_id: r.user_id, at: r.created_at });
  }
  return events;
}
async function metricsSummary({ userId = null, days = 14, weeks = 8 } = {}) {
  const tz = await timezone();
  const today = dayKey(now(), tz);
  const events = (await metricEvents()).filter((e) => !userId || e.user_id === userId);
  const dayKeys = Array.from({ length: days }, (_, i) => addDays(today, i - days + 1));
  const thisWeek = weekStart(today);
  const weekKeys = Array.from({ length: weeks }, (_, i) => addDays(thisWeek, (i - weeks + 1) * 7));
  const out = {};
  for (const m of METRICS) out[m.key] = { today: 0, total: 0, week: 0, daily: Object.fromEntries(dayKeys.map((k) => [k, 0])), weekly: Object.fromEntries(weekKeys.map((k) => [k, 0])) };
  for (const e of events) {
    const m = out[e.metric];
    const dk = dayKey(e.at, tz);
    const wk = weekStart(dk);
    m.total++;
    if (dk === today) m.today++;
    if (wk === thisWeek) m.week++;
    if (dk in m.daily) m.daily[dk]++;
    if (wk in m.weekly) m.weekly[wk]++;
  }
  const assistants = await all("SELECT * FROM users WHERE role = 'assistant' AND active = 1");
  const defaults = await defaultTargets();
  const scope = userId ? assistants.filter((a) => a.id === userId) : assistants;
  const targets = {};
  for (const m of METRICS) targets[m.key] = scope.reduce((s, a) => s + (Number(targetsFor(a, defaults)[m.key]) || 0), 0);
  return { today, timezone: tz, dayKeys, weekKeys, metrics: out, targets, definitions: METRICS };
}
async function checkTargets(user, metric) {
  if (user.role !== "assistant") return;
  const target = Number(targetsFor(user, await defaultTargets())[metric]) || 0;
  if (!target) return;
  const s = await metricsSummary({ userId: user.id, days: 1, weeks: 1 });
  const count = s.metrics[metric].today;
  if (count < target) return;
  const def = METRICS.find((m) => m.key === metric);
  await notify([...await ownerIds(), user.id], {
    type: "target",
    title: `Daily target reached: ${def.label.toLowerCase()}`,
    body: `${user.display_name} reached ${count} of ${target} ${def.label.toLowerCase()} today.`,
    dedupe_key: `target:${user.id}:${metric}:${s.today}`
  });
}
async function checkOverdue(force = false) {
  if (!force) {
    const last = await getSetting("last_overdue_check", 0);
    if (Date.now() - last < 6e4) return;
    await setSetting("last_overdue_check", Date.now());
    await run("DELETE FROM changes WHERE created_at < ?", [new Date(Date.now() - 864e5).toISOString()]);
  }
  const rows = await all(`SELECT * FROM leads WHERE status = 'Follow-up due' AND follow_up_due_at IS NOT NULL
    AND follow_up_due_at < ? AND overdue_notified_at IS NULL`, [now()]);
  for (const l of rows) {
    await run("UPDATE leads SET overdue_notified_at = ? WHERE id = ?", [now(), l.id]);
    await notify([...await ownerIds(), l.assigned_to], {
      type: "overdue",
      title: `Follow-up overdue: ${l.full_name}`,
      body: `The follow-up for ${leadLabel(l)} was due ${await fmtInTz(l.follow_up_due_at)}.`,
      lead_id: l.id,
      dedupe_key: `overdue:${l.id}:${l.follow_up_due_at}`
    });
  }
  if (rows.length) await broadcast("leads");
}
function httpError(status, message, extra) {
  const e = new Error(message);
  e.status = status;
  if (extra) e.extra = extra;
  return e;
}

// server/importer.js
import path2 from "node:path";
import ExcelJS from "exceljs";
import Papa from "papaparse";
import mammoth from "mammoth";
import { PDFParse } from "pdf-parse";
var FIELDS = [
  { key: "full_name", label: "Full name", syn: ["full name", "name", "contact name", "lead name", "contact", "person", "prospect"] },
  { key: "first_name", label: "First name", syn: ["first name", "firstname", "given name"] },
  { key: "last_name", label: "Last name", syn: ["last name", "lastname", "surname", "family name"] },
  { key: "title", label: "Position", syn: ["title", "position", "job title", "role", "current title", "headline", "designation", "job"] },
  { key: "linkedin_url", label: "LinkedIn URL", syn: ["linkedin", "linkedin url", "linkedin profile", "profile url", "li url", "linkedin profile url"] },
  { key: "company", label: "Company", syn: ["company", "brand", "company brand", "organisation", "organization", "account", "business", "company name"] },
  { key: "website", label: "Company website", syn: ["website", "company website", "domain", "site", "web", "company url", "website url"] },
  { key: "business_email", label: "Business email", syn: ["email", "business email", "work email", "public business email", "e mail", "email address"] },
  { key: "email_type", label: "Email type", syn: ["email type"] },
  { key: "email_source", label: "Email source", syn: ["email source", "email source url"] },
  { key: "city", label: "City", syn: ["city", "metro", "city metro", "town"] },
  { key: "country", label: "Country", syn: ["country"] },
  { key: "location", label: "Location (combined)", syn: ["location", "based in", "region", "geography"] },
  { key: "location_basis", label: "Location basis", syn: ["location basis"] },
  { key: "market", label: "Brand market", syn: ["brand market", "market", "markets"] },
  { key: "brand_category", label: "Brand category", syn: ["brand category", "category", "industry", "niche", "vertical", "segment"] },
  { key: "fit", label: "Fit classification", syn: ["fit", "classification", "qualification", "priority", "tier", "fit classification"] },
  { key: "priority", label: "Priority (high/medium/low)", syn: ["priority level", "urgency"] },
  { key: "fit_rationale", label: "Why the brand fits", syn: ["why the brand fits", "fit rationale", "rationale", "why", "reason", "fit reason"] },
  { key: "qualification_notes", label: "Qualification notes", syn: ["qualification notes", "notes", "comments", "comment"] },
  { key: "verification_notes", label: "Verification notes", syn: ["verification", "verification gaps", "gaps", "research notes"] },
  { key: "company_size", label: "Company size", syn: ["company size", "employees", "headcount", "size", "employee count"] },
  { key: "research_date", label: "Research date", syn: ["research date", "date researched", "researched"] },
  { key: "role_source", label: "Role source URL", syn: ["role source", "role source url"] },
  { key: "brand_source", label: "Brand source URL", syn: ["brand fit source", "brand source", "fit source"] },
  { key: "location_source", label: "Location source URL", syn: ["location source", "location source url"] },
  { key: "connection_note", label: "Outreach note", syn: ["outreach note", "connection note", "message", "outreach message", "note"] }
];
var FIELD_KEYS = new Set(FIELDS.map((f) => f.key));
var norm = (s) => String(s ?? "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
function scoreHeader(header) {
  const h = norm(header);
  if (!h) return { key: null, score: 0 };
  let best = { key: null, score: 0 };
  for (const f of FIELDS) {
    for (const syn of f.syn) {
      let s = 0;
      if (h === syn) s = 100 + syn.length;
      else if (new RegExp(`(^| )${syn}( |$)`).test(h)) s = syn.length;
      if (s > best.score) best = { key: f.key, score: s };
    }
  }
  return best;
}
function suggestMapping(headers) {
  const scored = headers.map((h, i) => ({ i, ...scoreHeader(h) })).sort((a, b) => b.score - a.score);
  const used = /* @__PURE__ */ new Set();
  const mapping = {};
  for (const s of scored) {
    if (s.key && !used.has(s.key)) {
      mapping[s.i] = s.key;
      used.add(s.key);
    }
  }
  return headers.map((_, i) => mapping[i] || "");
}
function detectHeaderRow(rows) {
  let best = { index: 0, score: -1 };
  rows.slice(0, 25).forEach((r, index) => {
    const filled = r.filter((c) => String(c ?? "").trim()).length;
    if (filled < 2) return;
    const matches = r.filter((c) => scoreHeader(c).score > 0).length;
    const score = matches * 10 + Math.min(filled, 5);
    if (matches >= 2 && score > best.score) best = { index, score };
  });
  if (best.score < 0) best.index = Math.max(0, rows.findIndex((r) => r.filter((c) => String(c ?? "").trim()).length >= 2));
  return best.index;
}
function cellText(v) {
  if (v === null || v === void 0) return "";
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === "object") {
    if (v.richText) return v.richText.map((t) => t.text).join("");
    if ("result" in v) return cellText(v.result);
    if (v.text !== void 0) return cellText(v.text);
    if (v.hyperlink) return v.hyperlink;
    if (v.error) return "";
    return "";
  }
  return String(v);
}
function stripHtml(s) {
  return s.replace(/<br\s*\/?>/gi, "\n").replace(/<\/p>/gi, "\n").replace(/<[^>]+>/g, "").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\n{2,}/g, "\n").trim();
}
function htmlTables(html) {
  const tables = [];
  for (const t of html.match(/<table[\s\S]*?<\/table>/gi) || []) {
    const rows = [];
    for (const tr of t.match(/<tr[\s\S]*?<\/tr>/gi) || []) {
      rows.push((tr.match(/<t[dh][\s\S]*?<\/t[dh]>/gi) || []).map((td) => {
        const link = td.match(/href="([^"]*linkedin\.com[^"]*)"/i);
        const text = stripHtml(td);
        return link && !/linkedin\.com/i.test(text) ? link[1] : text;
      }));
    }
    if (rows.length >= 2) tables.push(rows);
  }
  return tables;
}
var UnsupportedFileError = class extends Error {
};
async function parseFile(buf, originalName) {
  const ext = path2.extname(originalName).toLowerCase();
  if (ext === ".xls") throw new UnsupportedFileError("Older .xls files aren\u2019t supported. Save the file as .xlsx or .csv and upload it again.");
  if (ext === ".doc") throw new UnsupportedFileError("Older .doc files aren\u2019t supported. Save the file as .docx and upload it again.");
  if (ext === ".csv" || ext === ".tsv" || ext === ".txt" && /[,\t]/.test(buf.toString("utf8", 0, 2e3))) {
    const text = buf.toString("utf8").replace(/^﻿/, "");
    const res = Papa.parse(text, { skipEmptyLines: "greedy" });
    if (!res.data.length) throw new UnsupportedFileError("This CSV file is empty.");
    return tableResult("csv", [{ name: "Sheet 1", rows: res.data.map((r) => r.map((c) => String(c ?? ""))) }]);
  }
  if (ext === ".xlsx" || ext === ".xlsm") {
    const wb = new ExcelJS.Workbook();
    try {
      await wb.xlsx.load(buf);
    } catch {
      throw new UnsupportedFileError("This Excel file couldn\u2019t be read. It may be damaged or password protected.");
    }
    const sheets = [];
    wb.eachSheet((ws) => {
      const rows = [];
      ws.eachRow({ includeEmpty: true }, (row) => {
        const vals = [];
        for (let c = 1; c <= ws.columnCount; c++) vals.push(cellText(row.getCell(c).value).trim());
        rows.push(vals);
      });
      while (rows.length && rows[rows.length - 1].every((c) => !c)) rows.pop();
      if (rows.some((r) => r.some(Boolean))) sheets.push({ name: ws.name, rows });
    });
    if (!sheets.length) throw new UnsupportedFileError("This workbook doesn\u2019t contain any data.");
    return tableResult("excel", sheets);
  }
  if (ext === ".docx") {
    let html, text;
    try {
      html = (await mammoth.convertToHtml({ buffer: buf })).value;
      text = htmlToText(html);
    } catch {
      throw new UnsupportedFileError("This Word file couldn\u2019t be read. It may be damaged or password protected.");
    }
    const tables = htmlTables(html);
    const candidates = extractCandidates(text);
    if (tables.length) {
      const r = tableResult("word", tables.map((rows, i) => ({ name: `Table ${i + 1}`, rows })));
      r.review_required = true;
      r.text_candidates = candidates;
      return r;
    }
    return { kind: "document", file_kind: "word", review_required: true, candidates, text_length: text.length };
  }
  if (ext === ".pdf") {
    let text;
    try {
      const parser = new PDFParse({ data: new Uint8Array(buf) });
      text = (await parser.getText()).text;
      await parser.destroy?.();
    } catch {
      throw new UnsupportedFileError("This PDF couldn\u2019t be read. It may be scanned, damaged or password protected.");
    }
    if (!text || text.replace(/\s/g, "").length < 20) throw new UnsupportedFileError("No readable text was found in this PDF. Scanned PDFs need to be converted to text first.");
    return { kind: "document", file_kind: "pdf", review_required: true, candidates: extractCandidates(text), text_length: text.length };
  }
  throw new UnsupportedFileError("This file type isn\u2019t supported. Upload a CSV, Excel (.xlsx), Word (.docx) or PDF file.");
}
function tableResult(fileKind, sheets) {
  return {
    kind: "table",
    file_kind: fileKind,
    sheets: sheets.map((s) => {
      const width = Math.max(...s.rows.map((r) => r.length));
      const rows = s.rows.map((r) => Array.from({ length: width }, (_, i) => r[i] ?? ""));
      const headerRow = detectHeaderRow(rows);
      return { name: s.name, rows, header_row: headerRow, mapping: suggestMapping(rows[headerRow] || []) };
    })
  };
}
var LI_FIND = /(?:https?:\/\/)?(?:[a-z]{2,3}\.)?linkedin\.com\/(?:in|pub)\/[A-Za-z0-9\-_%.]+\/?/gi;
var URL_FIND = /\b(?:https?:\/\/)?(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:com|co|au|com\.au|io|net|org|shop|store|beauty|skin|us|co\.uk)(?:\/[^\s,;)]*)?/gi;
var TITLE_WORDS = /\b(founder|co-founder|cofounder|ceo|chief|director|manager|head of|vp|vice president|president|owner|lead|partner|officer|marketing|brand|creative|content|ecommerce|e-commerce|growth)\b/i;
var NAME_RE = /^(?:[A-Z][a-zA-Z'’\-]+)(?:\s+(?:[A-Z][a-zA-Z'’\-]+|[A-Z]\.)){1,3}$/;
var COUNTRIES = ["United States", "USA", "Australia", "United Kingdom", "Canada", "New Zealand", "Spain", "Singapore", "Philippines", "Germany", "France", "Ireland"];
function isNameLine(l) {
  return NAME_RE.test(l.replace(/^name\s*:\s*/i, "").trim()) && !TITLE_WORDS.test(l);
}
function extractCandidates(text) {
  const lines = text.replace(/\r/g, "").split("\n").map((l) => l.replace(/\s+$/, ""));
  const anchors = [];
  lines.forEach((l, i) => {
    if (LI_FIND.test(l)) anchors.push(i);
    LI_FIND.lastIndex = 0;
  });
  const blocks = [];
  if (anchors.length) {
    const lead = (from, to) => {
      let seg = lines.slice(from, to);
      const blank = seg.map((l) => !l.trim()).lastIndexOf(true);
      if (blank >= 0) seg = seg.slice(blank + 1);
      const nameAt = seg.findIndex(isNameLine);
      if (nameAt >= 0) seg = seg.slice(nameAt);
      return seg.slice(-6);
    };
    anchors.forEach((a, k) => {
      const prevEnd = k === 0 ? Math.max(0, a - 8) : anchors[k - 1] + 1;
      const nextStart = k === anchors.length - 1 ? Math.min(lines.length, a + 5) : anchors[k + 1];
      const after = [];
      for (let i = a + 1; i < nextStart; i++) {
        const l = lines[i];
        if (!l.trim() || isNameLine(l)) break;
        if (k < anchors.length - 1 && TITLE_WORDS.test(l) && !EMAIL_RE.test(l)) break;
        after.push(l);
      }
      const before = k === 0 ? lead(prevEnd, a) : lead(prevEnd + blocksAfterLen(k - 1), a);
      blocks.push({ lines: [...before, lines[a], ...after], afterLen: after.length });
      function blocksAfterLen(j) {
        return blocks[j] ? blocks[j].afterLen : 0;
      }
    });
  } else {
    for (const b of text.split(/\n\s*\n/)) if (EMAIL_RE.test(b)) blocks.push({ lines: b.split("\n") });
  }
  const out = [];
  for (const { lines: bl } of blocks) {
    const block = bl.filter((l) => l.trim()).join("\n");
    const li = block.match(LI_FIND);
    LI_FIND.lastIndex = 0;
    const email = block.match(EMAIL_RE);
    const parts = bl.flatMap((l) => l.split(/\t|\s{3,}|\s[|•–—]\s/)).map((x) => x.trim()).filter(Boolean);
    const cand = { full_name: "", title: "", company: "", linkedin_url: li ? li[0] : "", business_email: email ? email[0] : "", website: "", country: "" };
    for (const p of parts) {
      const bare = p.replace(LI_FIND, "").replace(EMAIL_RE, "").replace(/^(name|title|position|company|brand|email|linkedin|website)\s*:\s*/i, "").trim();
      LI_FIND.lastIndex = 0;
      if (!bare) continue;
      const label = (p.match(/^(name|title|position|company|brand|website)\s*:/i) || [])[1]?.toLowerCase();
      if (label === "name" && !cand.full_name) {
        cand.full_name = bare;
        continue;
      }
      if ((label === "title" || label === "position") && !cand.title) {
        cand.title = bare;
        continue;
      }
      if ((label === "company" || label === "brand") && !cand.company) {
        cand.company = bare;
        continue;
      }
      if (label === "website" && !cand.website) {
        cand.website = bare;
        continue;
      }
      const at = bare.match(/^(.*?)\s+(?:at|@)\s+(.+)$/i);
      if (at && TITLE_WORDS.test(at[1]) && !cand.title) {
        cand.title = at[1].trim();
        cand.company ||= at[2].trim();
        continue;
      }
      if (!cand.full_name && isNameLine(bare)) {
        cand.full_name = bare;
        continue;
      }
      if (!cand.title && TITLE_WORDS.test(bare) && bare.length < 80) {
        cand.title = bare;
        continue;
      }
      const site = bare.match(URL_FIND);
      if (site && !cand.website && !/linkedin/i.test(site[0]) && !/\s/.test(bare)) {
        cand.website = site[0];
        continue;
      }
      const country = COUNTRIES.find((c) => new RegExp(`\\b${c}\\b`, "i").test(bare));
      if (country && !cand.country) cand.country = country === "USA" ? "United States" : country;
    }
    cand._context = block.slice(0, 400);
    out.push(cand);
  }
  return out;
}
function htmlToText(html) {
  return html.replace(/<br\s*\/?>/gi, "\n").replace(/<\/(p|h[1-6]|li|tr)>/gi, "\n\n").replace(/<\/t[dh]>/gi, "	").replace(/<[^>]+>/g, "").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'");
}
var GAP_FIELDS = /* @__PURE__ */ new Set(["full_name", "title", "company", "website", "business_email", "city", "country", "brand_category"]);
var MISSING = /^(not found|not confirmed|n\/?a|none|unknown|tbc|tbd|-|—|\?)$/i;
function cleanValue(v) {
  const s = String(v ?? "").replace(/\s+/g, " ").trim();
  return s;
}
function recordsFromTable(sheet, headerRow, mapping) {
  const rows = sheet.rows.slice(headerRow + 1);
  const headers = sheet.rows[headerRow] || [];
  const records = [];
  rows.forEach((r, i) => {
    if (!r.some((c) => String(c ?? "").trim())) return;
    const rec = { _row: headerRow + i + 2, _extra: {} };
    r.forEach((val, ci) => {
      const key = mapping[ci];
      const v = cleanValue(val);
      if (key && FIELD_KEYS.has(key)) {
        if (v && !rec[key]) rec[key] = v;
      } else if (v && headers[ci]) rec._extra[cleanValue(headers[ci])] = v;
    });
    records.push(rec);
  });
  return records;
}
function buildLead(raw2) {
  const lead = {};
  const gaps = {};
  const errors = [];
  const warnings = [];
  const take = (k) => {
    const v = cleanValue(raw2[k]);
    if (!v) return null;
    if (MISSING.test(v)) {
      if (GAP_FIELDS.has(k)) gaps[k] = v;
      return null;
    }
    return v;
  };
  lead.full_name = take("full_name") || [take("first_name"), take("last_name")].filter(Boolean).join(" ") || null;
  for (const k of [
    "title",
    "company",
    "website",
    "business_email",
    "email_type",
    "email_source",
    "city",
    "country",
    "location_basis",
    "market",
    "brand_category",
    "priority",
    "fit_rationale",
    "qualification_notes",
    "verification_notes",
    "company_size",
    "research_date",
    "connection_note"
  ]) lead[k] = take(k);
  const loc = take("location");
  if (loc && !lead.city && !lead.country) {
    const bits = loc.split(",").map((s) => s.trim()).filter(Boolean);
    if (bits.length > 1) {
      lead.country = bits.pop();
      lead.city = bits.join(", ");
    } else lead.city = loc;
  }
  if (lead.email_type && /not found/i.test(lead.email_type)) lead.email_type = null;
  if (lead.website && !/^https?:\/\//i.test(lead.website)) lead.website = `https://${lead.website}`;
  if (lead.business_email && !EMAIL_RE.test(lead.business_email)) {
    warnings.push(`Email \u201C${lead.business_email}\u201D doesn\u2019t look valid and was left out`);
    gaps.business_email = "Unclear";
    lead.business_email = null;
  }
  if (lead.priority) {
    const p = lead.priority.toLowerCase();
    lead.priority = /high/.test(p) ? "High" : /med/.test(p) ? "Medium" : /low/.test(p) ? "Low" : null;
  }
  const sources = {};
  for (const k of ["role_source", "brand_source", "location_source"]) {
    const v = take(k);
    if (v) sources[k] = v;
  }
  lead.sources = Object.keys(sources).length ? sources : null;
  lead.extra = raw2._extra && Object.keys(raw2._extra).length ? raw2._extra : null;
  const rawFit = take("fit");
  const fromFile = normaliseFit(rawFit);
  if (fromFile) {
    lead.fit = fromFile;
    lead.fit_basis = "source";
  } else {
    const s = suggestFit(lead);
    lead.fit = s.fit;
    lead.fit_basis = s.fit ? "suggested" : null;
    if (s.fit) warnings.push(`Fit suggested from the brief: ${s.fit} (${s.reason.toLowerCase()})`);
    else warnings.push("Fit needs review");
  }
  if (!lead.full_name) errors.push("Name is missing");
  const li = parseLinkedIn(raw2.linkedin_url);
  if (!li.ok) errors.push(li.error);
  else {
    lead.linkedin_url = li.url;
    lead.linkedin_key = li.key;
  }
  lead.gaps = gaps;
  for (const [k, v] of Object.entries(gaps)) {
    const label = FIELDS.find((f) => f.key === k)?.label || k;
    warnings.push(`${label}: ${v.toLowerCase()}`);
  }
  if (!lead.business_email && !gaps.business_email) warnings.push("No business email");
  if (!lead.city && !lead.country && !gaps.city && !gaps.country) warnings.push("No location");
  return { lead, errors, warnings };
}
async function validateRecords(records) {
  const existingByKey = new Map((await all("SELECT id, full_name, linkedin_key FROM leads")).map((r) => [r.linkedin_key, r]));
  const existingEmails = /* @__PURE__ */ new Map();
  for (const r of await all("SELECT id, full_name, business_email FROM leads WHERE business_email IS NOT NULL")) {
    const k = r.business_email.toLowerCase();
    if (!existingEmails.has(k)) existingEmails.set(k, []);
    existingEmails.get(k).push(r.full_name);
  }
  const seenKeys = /* @__PURE__ */ new Map();
  const fileEmails = /* @__PURE__ */ new Map();
  const built = records.map((r) => ({ raw: r, ...buildLead(r) }));
  for (const b of built) {
    const e = b.lead.business_email?.toLowerCase();
    if (e) {
      if (!fileEmails.has(e)) fileEmails.set(e, []);
      fileEmails.get(e).push(b.lead.full_name || "Unnamed");
    }
  }
  return built.map((b, index) => {
    const out = { index, row: b.raw._row ?? null, lead: b.lead, errors: [...b.errors], warnings: [...b.warnings], state: "ready", duplicate_of: null };
    const key = b.lead.linkedin_key;
    if (key && existingByKey.has(key)) {
      out.state = "duplicate";
      out.duplicate_of = existingByKey.get(key).full_name;
      out.errors.unshift(`Already in the database as ${existingByKey.get(key).full_name}`);
    } else if (key && seenKeys.has(key)) {
      out.state = "duplicate";
      out.duplicate_of = seenKeys.get(key);
      out.errors.unshift(`Same LinkedIn profile appears earlier in this file (row ${seenKeys.get(key)})`);
    } else if (out.errors.length) {
      out.state = "error";
    }
    if (key && !seenKeys.has(key)) seenKeys.set(key, b.raw._row ?? index + 1);
    const email = b.lead.business_email?.toLowerCase();
    if (email) {
      const named = /named|personal|direct/i.test(b.lead.email_type || "");
      const inDb = existingEmails.get(email) || [];
      const inFile = (fileEmails.get(email) || []).filter((n) => n !== b.lead.full_name);
      const others = [.../* @__PURE__ */ new Set([...inDb, ...inFile])];
      if (others.length) {
        out.warnings.unshift(named ? `Possible duplicate person: named email also used by ${others.join(", ")}` : `Shared inbox also used by ${others.join(", ")}`);
      }
    }
    return out;
  });
}
async function commitImport(importRow, records, user, { assignTo } = {}) {
  const results = await validateRecords(records);
  const templates = { ...DEFAULT_TEMPLATES, ...await getSetting("templates", {}) };
  const assignee = user.role === "assistant" ? user.id : assignTo || null;
  const added = [];
  const skipped = [];
  const needCorrection = [];
  const excluded = [];
  let flagged = 0;
  await tx(async () => {
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      const include = records[i]._include !== false;
      const label = r.lead.full_name || `Row ${r.row ?? i + 1}`;
      if (r.state === "duplicate") {
        skipped.push({ name: label, row: r.row, reason: r.errors[0] });
        continue;
      }
      if (r.state === "error") {
        needCorrection.push({ name: label, row: r.row, reason: r.errors.join("; ") });
        continue;
      }
      if (!include) {
        excluded.push({ name: label, row: r.row, reason: "Not approved in review" });
        continue;
      }
      const l = r.lead;
      const t = now();
      const status = assignee ? "Assigned" : "New";
      const res = await get(
        `INSERT INTO leads (full_name, title, linkedin_url, linkedin_key, company, website, city, country,
        market, location_basis, brand_category, fit, fit_basis, priority, assigned_to, status, status_updated_at, status_updated_by,
        connection_note, followup_note, company_size, business_email, email_type, email_source, qualification_notes,
        fit_rationale, verification_notes, research_date, sources, gaps, extra, import_id, created_by, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT (linkedin_key) DO NOTHING RETURNING id`,
        [
          l.full_name,
          l.title,
          l.linkedin_url,
          l.linkedin_key,
          l.company,
          l.website,
          l.city,
          l.country,
          l.market,
          l.location_basis,
          l.brand_category,
          l.fit,
          l.fit_basis,
          l.priority,
          assignee,
          status,
          t,
          user.id,
          l.connection_note || renderTemplate(templates.connection, l),
          renderTemplate(templates.followup, l),
          l.company_size,
          l.business_email,
          l.email_type,
          l.email_source,
          l.qualification_notes,
          l.fit_rationale,
          l.verification_notes,
          l.research_date,
          l.sources ? JSON.stringify(l.sources) : null,
          JSON.stringify(l.gaps || {}),
          l.extra ? JSON.stringify(l.extra) : null,
          importRow.id,
          user.id,
          t,
          t
        ]
      );
      if (!res) {
        skipped.push({ name: label, row: r.row, reason: "Added by someone else a moment ago" });
        continue;
      }
      await logActivity(res.id, user.id, "imported", { to: status, detail: { file: importRow.filename, row: r.row } });
      const hasGaps = Object.keys(l.gaps || {}).length > 0 || !l.business_email;
      if (hasGaps) flagged++;
      added.push({ id: res.id, name: l.full_name, row: r.row, flagged: hasGaps });
    }
    const summary2 = {
      total: records.length,
      added: added.length,
      skipped: skipped.length + excluded.length,
      need_correction: needCorrection.length,
      flagged_gaps: flagged,
      duplicates: skipped,
      excluded,
      corrections: needCorrection,
      added_leads: added
    };
    await run(
      "UPDATE imports SET status = 'completed', summary = ?, completed_at = ?, parsed = NULL WHERE id = ?",
      [JSON.stringify(summary2), now(), importRow.id]
    );
  });
  const summary = {
    total: records.length,
    added: added.length,
    skipped: skipped.length + excluded.length,
    need_correction: needCorrection.length,
    flagged_gaps: flagged,
    duplicates: skipped,
    excluded,
    corrections: needCorrection,
    added_leads: added
  };
  await notify([...await ownerIds(), user.id], {
    type: "import",
    title: `File import finished: ${importRow.filename}`,
    body: `${summary.added} added, ${summary.skipped} skipped, ${summary.need_correction} need correction${flagged ? `, ${flagged} added with missing details flagged` : ""}.`,
    import_id: importRow.id
  });
  if (assignee && assignee !== user.id && added.length) {
    await notify([assignee], { type: "assigned", title: `${added.length} new ${added.length === 1 ? "lead" : "leads"} assigned to you`, body: `From ${importRow.filename}.` });
  }
  await broadcast("leads", "imports", "activity", "metrics");
  return summary;
}
async function createImport(user, filename, kind, parsed) {
  const r = await get(
    "INSERT INTO imports (user_id, filename, file_kind, status, parsed, created_at) VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
    [user.id, filename, kind, "preview", JSON.stringify(parsed), now()]
  );
  return r.id;
}
function recordsForImport(importRow, body) {
  const parsed = JSON.parse(importRow.parsed || "null");
  if (!parsed) throw httpError(409, "This import has already been completed or cancelled.");
  if (parsed.kind === "table" && body.mode !== "candidates") {
    const sheet = parsed.sheets[Number(body.sheet) || 0];
    if (!sheet) throw httpError(400, "Choose a sheet.");
    const headerRow = Math.max(0, Math.min(Number(body.header_row ?? sheet.header_row), sheet.rows.length - 1));
    const mapping = Array.isArray(body.mapping) ? body.mapping.map((k) => FIELD_KEYS.has(k) ? k : "") : sheet.mapping;
    if (!mapping.includes("linkedin_url")) throw httpError(400, "Map a column to LinkedIn URL before continuing.");
    if (!mapping.some((k) => ["full_name", "first_name"].includes(k))) throw httpError(400, "Map a column to Full name (or First name) before continuing.");
    return recordsFromTable(sheet, headerRow, mapping);
  }
  const recs = Array.isArray(body.records) ? body.records : [];
  return recs.slice(0, 5e3).map((r, i) => {
    const rec = { _row: r._row ?? i + 1, _extra: {} };
    for (const k of FIELD_KEYS) if (r[k] !== void 0) rec[k] = String(r[k] ?? "");
    return rec;
  });
}
function applyReview(records, body, reviewRequired) {
  const overrides = body.overrides && typeof body.overrides === "object" ? body.overrides : {};
  const include = Array.isArray(body.include) ? new Set(body.include.map(Number)) : null;
  return records.map((r, i) => {
    const o = overrides[i];
    const rec = { ...r };
    if (o && typeof o === "object") {
      for (const k of FIELD_KEYS) if (o[k] !== void 0) rec[k] = String(o[k] ?? "");
    }
    rec._include = include ? include.has(i) : !reviewRequired;
    return rec;
  });
}

// server/seed.js
import fs from "node:fs";
import path3 from "node:path";

// server/seed-data.js
var SEED_FILES = [{ "name": "HelloGraciela \u2014 Lead Research 18 of 1000 Incomplete 1.xlsx", "base64": "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w87gsDM47AwOO4PDzuCwMzjsDA47g8PO4LAzOOwMDjuDw87gsDM47AwOO4PDzuCwMzjsDA47g8PO4LAzOOwMDjuDw87gsDM47AwOO4PDzuCwM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47Q/P/UQ2frnrsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDB47g8fO4LEzeOwMHjuDx87gsTN47AweO4PHzuCxM3jsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMATtDwM4QsDME7AwBO0PAzhCwMwTsDAE7Q8DOELAzBOwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwMETtDxM4QsTNE7AwRO0PEzhCxM0TsDBE7Q8TOELEzROwM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" }];

// server/seed.js
function seedFiles() {
  if (process.env.SEED_DIR && fs.existsSync(process.env.SEED_DIR)) {
    return fs.readdirSync(process.env.SEED_DIR).filter((f) => /\.(xlsx|csv)$/i.test(f)).map((name) => ({ name, buffer: fs.readFileSync(path3.join(process.env.SEED_DIR, name)) }));
  }
  return SEED_FILES.map((f) => ({ name: f.name, buffer: Buffer.from(f.base64, "base64") }));
}
var done = null;
function ensureSetup() {
  if (!done) done = setup().catch((e) => {
    done = null;
    throw e;
  });
  return done;
}
async function setup() {
  await init();
  const claim = await get("INSERT INTO settings (key, value) VALUES ('setup_claimed', ?) ON CONFLICT (key) DO NOTHING RETURNING key", [JSON.stringify(now())]);
  if (!claim) {
    for (let i = 0; i < 40; i++) {
      if ((await get("SELECT COUNT(*)::int AS n FROM users")).n) return;
      await new Promise((r) => setTimeout(r, 250));
    }
    return;
  }
  try {
    if (!(await get("SELECT COUNT(*)::int AS n FROM users")).n) {
      const accounts = [
        { username: process.env.OWNER_USERNAME || "HELLOGRACIELA", password: process.env.OWNER_INITIAL_PASSWORD || "hello2026", role: "owner", name: "Owner" },
        { username: process.env.ASSISTANT_USERNAME || "Helloassist", password: process.env.ASSISTANT_INITIAL_PASSWORD || "Helloassist2026", role: "assistant", name: "Assistant" }
      ];
      for (const a of accounts) {
        await run(
          "INSERT INTO users (username, display_name, role, password_hash, must_change, created_at) VALUES (?, ?, ?, ?, 1, ?)",
          [a.username, a.name, a.role, hashPassword(a.password), now()]
        );
      }
      console.log("Created the initial owner and assistant accounts. Change their passwords before going live.");
    }
    const counts = await get("SELECT (SELECT COUNT(*) FROM leads)::int AS leads, (SELECT COUNT(*) FROM imports)::int AS imports");
    if (counts.leads || counts.imports || process.env.SKIP_SEED_IMPORT === "true") return;
    const owner = await get("SELECT * FROM users WHERE role = 'owner' ORDER BY id LIMIT 1");
    const assistant = await get("SELECT * FROM users WHERE role = 'assistant' ORDER BY id LIMIT 1");
    for (const { name: file, buffer } of seedFiles()) {
      const parsed = await parseFile(buffer, file);
      const id = await createImport(owner, file, parsed.file_kind, parsed);
      const row = await get("SELECT * FROM imports WHERE id = ?", [id]);
      const records = applyReview(recordsForImport(row, { sheet: 0 }), {}, false);
      const summary = await commitImport(row, records, owner, { assignTo: assistant?.id });
      console.log(`Imported ${file}: ${summary.added} added, ${summary.skipped} skipped, ${summary.need_correction} need correction, ${summary.flagged_gaps} flagged for missing details.`);
    }
  } catch (e) {
    await run("DELETE FROM settings WHERE key = 'setup_claimed'").catch(() => {
    });
    throw e;
  }
}

// server/app.js
var PROD = process.env.NODE_ENV === "production";
var MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB || 4);
var FIELD_LIST = FIELDS.map(({ key, label }) => ({ key, label }));
var app = express();
app.disable("x-powered-by");
if (process.env.VERCEL || process.env.TRUST_PROXY) app.set("trust proxy", process.env.TRUST_PROXY && process.env.TRUST_PROXY !== "true" ? process.env.TRUST_PROXY : 1);
app.use((req, res, next) => {
  res.setHeader("Content-Security-Policy", [
    "default-src 'self'",
    "script-src 'self'",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "font-src 'self' https://fonts.gstatic.com",
    "img-src 'self' data:",
    "connect-src 'self'",
    "frame-ancestors 'none'",
    "base-uri 'self'",
    "form-action 'self'"
  ].join("; "));
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  if (req.path.startsWith("/api")) res.setHeader("Cache-Control", "no-store");
  if (PROD) res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  next();
});
app.use("/api", express.json({ limit: "4mb" }));
app.use("/api", async (req, res, next) => {
  try {
    await ensureSetup();
  } catch (e) {
    console.error(e);
    const noDb = /DATABASE_URL/.test(e.message);
    return res.status(503).json({
      code: noDb ? "no_database" : "database_error",
      error: noDb ? "The database isn\u2019t connected yet. In Vercel, open this project\u2019s Storage tab, add a Neon Postgres database, then redeploy." : "The database couldn\u2019t be reached. Please try again in a minute."
    });
  }
  const token = parseCookies(req.headers.cookie)[COOKIE];
  req.token = token;
  req.user = await sessionUser(token);
  next();
});
app.use("/api", (req, res, next) => {
  if (["GET", "HEAD", "OPTIONS"].includes(req.method)) return next();
  if (req.get("X-Obsidia") !== "1") return res.status(403).json({ error: "Request blocked." });
  next();
});
var setupRequired = (u) => PROD && !!u.must_change;
var anyOwnerNeedsSetup = async () => !!await get("SELECT 1 AS x FROM users WHERE role = 'owner' AND must_change = 1 AND active = 1");
var isBlocked = async (u) => setupRequired(u) || PROD && u.role === "assistant" && await anyOwnerNeedsSetup();
async function requireUser(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Please sign in." });
  if (await isBlocked(req.user)) {
    return res.status(403).json({ error: req.user.role === "owner" ? "Update the initial credentials to continue." : "Access opens once the owner finishes account setup.", code: "setup_required" });
  }
  next();
}
function requireOwner(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Please sign in." });
  if (req.user.role !== "owner") return res.status(403).json({ error: "Only the owner can do that." });
  next();
}
function setCookie(res, token, expires) {
  res.setHeader("Set-Cookie", `${COOKIE}=${token}; Path=/; HttpOnly; SameSite=Lax; ${PROD ? "Secure; " : ""}Expires=${new Date(expires).toUTCString()}`);
}
app.post("/api/auth/login", async (req, res) => {
  const { username, password } = req.body || {};
  const ip = req.ip || "unknown";
  const keys = [`ip:${ip}`, `u:${String(username || "").toLowerCase()}`];
  if (await tooManyAttempts(keys)) return res.status(429).json({ error: "Too many sign-in attempts. Please wait 15 minutes and try again." });
  const user = await checkCredentials(username, password);
  if (!user) {
    await recordFailure(keys);
    return res.status(401).json({ error: "That username and password combination didn\u2019t work. Please try again." });
  }
  await clearFailures(keys);
  const { token, expires } = await createSession(user.id);
  setCookie(res, token, expires);
  res.json({ user: publicUser(user) });
});
app.post("/api/auth/logout", async (req, res) => {
  await destroySession(req.token);
  res.setHeader("Set-Cookie", `${COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
  res.json({ ok: true });
});
app.get("/api/me", async (req, res) => {
  if (!req.user) return res.status(401).json({ error: "Please sign in." });
  const initialInUse = await all("SELECT username, role FROM users WHERE must_change = 1 AND active = 1");
  res.json({
    user: publicUser(req.user),
    production: PROD,
    setup_required: await isBlocked(req.user),
    initial_credentials_in_use: req.user.role === "owner" ? initialInUse.map((u) => u.role) : void 0,
    timezone: await timezone(),
    constants: { statuses: STATUSES, fits: FITS, priorities: PRIORITIES, metrics: METRICS }
  });
});
app.get("/api/changes", requireUser, async (req, res) => {
  await checkOverdue();
  const after = req.query.after === void 0 ? null : Number(req.query.after);
  res.json(await changesSince(after));
});
async function visibleLeads(user) {
  const rows = user.role === "owner" ? await all("SELECT * FROM leads ORDER BY id") : await all("SELECT * FROM leads WHERE assigned_to = ? ORDER BY id", [user.id]);
  const names = await userNames();
  return rows.map((r) => serializeLead(r, names));
}
app.get("/api/leads", requireUser, async (req, res) => res.json({ leads: await visibleLeads(req.user) }));
app.get("/api/leads/:id", requireUser, async (req, res) => {
  const lead = await getLead(Number(req.params.id));
  if (!canAccessLead(req.user, lead)) return res.status(404).json({ error: "Lead not found." });
  res.json({ lead: serializeLead(lead, await userNames()), activity: await leadActivity(lead.id) });
});
var ASSISTANT_EDITABLE = ["connection_note", "followup_note", "qualification_notes", "company_size", "business_email", "email_source", "fit"];
var OWNER_EDITABLE = [
  ...ASSISTANT_EDITABLE,
  "full_name",
  "title",
  "linkedin_url",
  "company",
  "website",
  "city",
  "country",
  "market",
  "brand_category",
  "priority",
  "email_type",
  "fit_rationale",
  "verification_notes"
];
app.patch("/api/leads/:id", requireUser, async (req, res) => {
  const lead = await getLead(Number(req.params.id));
  if (!canAccessLead(req.user, lead)) return res.status(404).json({ error: "Lead not found." });
  const allowed = req.user.role === "owner" ? OWNER_EDITABLE : ASSISTANT_EDITABLE;
  const body = req.body || {};
  const changes = {};
  for (const k of Object.keys(body)) {
    if (!allowed.includes(k)) continue;
    let v = body[k] === null ? null : String(body[k]).trim().slice(0, 5e3);
    if (v === "") v = null;
    if (k === "fit" && v && !FITS.includes(v)) return res.status(400).json({ error: "Choose a valid fit classification." });
    if (k === "priority" && v && !PRIORITIES.includes(v)) return res.status(400).json({ error: "Choose a valid priority." });
    if (k === "business_email" && v && !EMAIL_RE.test(v)) return res.status(400).json({ error: "That email address doesn\u2019t look right." });
    if (k === "full_name" && !v) return res.status(400).json({ error: "Name is required." });
    if (k === "linkedin_url") {
      const li = parseLinkedIn(v);
      if (!li.ok) return res.status(400).json({ error: li.error });
      const clash = await get("SELECT full_name FROM leads WHERE linkedin_key = ? AND id <> ?", [li.key, lead.id]);
      if (clash) return res.status(409).json({ error: `That LinkedIn profile already belongs to ${clash.full_name}.` });
      v = li.url;
      if (li.key !== lead.linkedin_key) changes.linkedin_key = li.key;
    }
    if (k === "website" && v && !/^https?:\/\//i.test(v)) v = `https://${v}`;
    if ((lead[k] ?? null) !== v) changes[k] = v;
  }
  const keys = Object.keys(changes);
  if (!keys.length) return res.json({ lead: serializeLead(lead, await userNames()) });
  if (changes.fit !== void 0) changes.fit_basis = "manual";
  if (changes.business_email !== void 0 || changes.city !== void 0 || changes.country !== void 0) {
    const gaps = lead.gaps ? JSON.parse(lead.gaps) : {};
    for (const k of ["business_email", "city", "country"]) if (changes[k]) delete gaps[k];
    changes.gaps = JSON.stringify(gaps);
  }
  const t = now();
  await tx(async () => {
    const sets = Object.keys(changes).map((k) => `${k} = ?`).join(", ");
    await run(`UPDATE leads SET ${sets}, updated_at = ? WHERE id = ?`, [...Object.values(changes), t, lead.id]);
    const fields = keys.filter((k) => !["linkedin_key", "gaps", "fit_basis"].includes(k));
    const noteOnly = fields.every((k) => ["connection_note", "followup_note"].includes(k));
    await logActivity(lead.id, req.user.id, noteOnly ? "note_edited" : "edited", { detail: { fields } });
  });
  await broadcast("leads", "activity");
  res.json({ lead: serializeLead(await getLead(lead.id), await userNames()) });
});
app.delete("/api/leads/:id", requireOwner, requireUser, async (req, res) => {
  const lead = await getLead(Number(req.params.id));
  if (!lead) return res.status(404).json({ error: "Lead not found." });
  await run("DELETE FROM leads WHERE id = ?", [lead.id]);
  await broadcast("leads", "activity", "metrics");
  res.json({ ok: true });
});
app.post("/api/leads/:id/open", requireUser, async (req, res) => {
  res.json(await recordProfileOpen(Number(req.params.id), req.user));
});
app.post("/api/leads/:id/status", requireUser, async (req, res) => {
  const { status, follow_up_due_at, meeting_at, reason } = req.body || {};
  res.json(await setStatus(Number(req.params.id), status, req.user, { follow_up_due_at, meeting_at, reason }));
});
app.post("/api/leads/:id/followup-sent", requireUser, async (req, res) => {
  res.json(await recordFollowupSent(Number(req.params.id), req.user, { next_due_at: req.body?.next_due_at }));
});
app.post("/api/leads/:id/copied", requireUser, async (req, res) => {
  const lead = await getLead(Number(req.params.id));
  if (!canAccessLead(req.user, lead)) return res.status(404).json({ error: "Lead not found." });
  const kind = req.body?.kind === "followup" ? "followup" : "connection";
  await logActivity(lead.id, req.user.id, "note_copied", { detail: { kind } });
  await broadcast("activity");
  res.json({ ok: true });
});
app.post("/api/leads/:id/reset-note", requireUser, async (req, res) => {
  const lead = await getLead(Number(req.params.id));
  if (!canAccessLead(req.user, lead)) return res.status(404).json({ error: "Lead not found." });
  const kind = req.body?.kind === "followup" ? "followup" : "connection";
  const templates = { ...DEFAULT_TEMPLATES, ...await getSetting("templates", {}) };
  res.json({ text: renderTemplate(templates[kind], lead) });
});
app.post("/api/activity/:id/undo", requireUser, async (req, res) => {
  await undoActivity(Number(req.params.id), req.user);
  res.json({ ok: true });
});
app.post("/api/leads/assign", requireOwner, requireUser, async (req, res) => {
  const ids = (req.body?.ids || []).map(Number).filter(Boolean);
  if (!ids.length) return res.status(400).json({ error: "Select at least one lead." });
  const changed = await assignLeads(ids, req.body.assistant_id ? Number(req.body.assistant_id) : null, req.user);
  res.json({ changed });
});
app.get("/api/activity", requireOwner, requireUser, async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 60, 300);
  const names = await userNames();
  const rows = await all(`SELECT a.*, l.full_name AS lead_name, l.company AS lead_company FROM activity a
    LEFT JOIN leads l ON l.id = a.lead_id ORDER BY a.created_at DESC, a.id DESC LIMIT ?`, [limit]);
  res.json({ activity: rows.map((a) => ({ ...a, detail: a.detail ? JSON.parse(a.detail) : null, user_name: names[a.user_id] || "System" })) });
});
app.get("/api/metrics", requireOwner, requireUser, async (req, res) => {
  const userId = req.query.assistant ? Number(req.query.assistant) : null;
  res.json(await metricsSummary({ userId }));
});
app.get("/api/my-progress", requireUser, async (req, res) => {
  const s = await metricsSummary({ userId: req.user.id, days: 1, weeks: 1 });
  const targets = req.user.role === "assistant" ? targetsFor(req.user, await defaultTargets()) : s.targets;
  res.json({
    today: s.today,
    timezone: s.timezone,
    metrics: METRICS.map((m) => ({ key: m.key, label: m.label, today: s.metrics[m.key].today, target: Number(targets[m.key]) || 0 }))
  });
});
app.get("/api/notifications", requireUser, async (req, res) => {
  const rows = await all("SELECT * FROM notifications WHERE recipient_id = ? ORDER BY created_at DESC, id DESC LIMIT 100", [req.user.id]);
  const unread = (await get("SELECT COUNT(*)::int AS n FROM notifications WHERE recipient_id = ? AND read_at IS NULL", [req.user.id])).n;
  res.json({ notifications: rows, unread });
});
app.post("/api/notifications/read", requireUser, async (req, res) => {
  const t = now();
  if (req.body?.all) await run("UPDATE notifications SET read_at = ? WHERE recipient_id = ? AND read_at IS NULL", [t, req.user.id]);
  else for (const id of (req.body?.ids || []).map(Number)) await run("UPDATE notifications SET read_at = ? WHERE id = ? AND recipient_id = ? AND read_at IS NULL", [t, id, req.user.id]);
  await broadcast("notifications");
  res.json({ ok: true });
});
async function usersPayload() {
  const counts = Object.fromEntries((await all("SELECT assigned_to, COUNT(*)::int AS n FROM leads WHERE assigned_to IS NOT NULL GROUP BY assigned_to")).map((r) => [r.assigned_to, r.n]));
  const defaults = await defaultTargets();
  return (await all("SELECT * FROM users ORDER BY role DESC, id")).map((u) => ({
    ...publicUser(u),
    targets: u.role === "assistant" ? targetsFor(u, defaults) : null,
    lead_count: counts[u.id] || 0,
    password_changed_at: u.password_changed_at
  }));
}
app.get("/api/users", async (req, res, next) => {
  if (req.user?.role === "owner") return res.json({ users: await usersPayload() });
  await requireUser(req, res, () => res.json({ users: [publicUser(req.user)] }));
});
app.post("/api/users", requireOwner, requireUser, async (req, res) => {
  const { username, display_name, password } = req.body || {};
  const e = validateUsername(username) || validatePassword(password);
  if (e) return res.status(400).json({ error: e });
  if (!display_name || !String(display_name).trim()) return res.status(400).json({ error: "Add a display name." });
  if (await get("SELECT 1 AS x FROM users WHERE lower(username) = lower(?)", [username.trim()])) return res.status(409).json({ error: "That username is already taken." });
  await run(`INSERT INTO users (username, display_name, role, password_hash, must_change, created_at, password_changed_at)
    VALUES (?, ?, 'assistant', ?, 0, ?, ?)`, [username.trim(), String(display_name).trim().slice(0, 60), hashPassword(password), now(), now()]);
  await broadcast("users", "metrics");
  res.json({ users: await usersPayload() });
});
app.put("/api/users/:id", requireOwner, requireUser, async (req, res) => {
  const u = await get("SELECT * FROM users WHERE id = ?", [Number(req.params.id)]);
  if (!u) return res.status(404).json({ error: "User not found." });
  const { display_name, active, targets } = req.body || {};
  if (targets && u.role === "assistant") {
    const clean = {};
    for (const m of METRICS) {
      const n = Number(targets[m.key]);
      if (!Number.isFinite(n) || n < 0 || n > 1e4) return res.status(400).json({ error: `Enter a target between 0 and 10,000 for ${m.label.toLowerCase()}.` });
      clean[m.key] = Math.round(n);
    }
    await run("UPDATE users SET targets = ? WHERE id = ?", [JSON.stringify(clean), u.id]);
  }
  if (display_name !== void 0) {
    if (!String(display_name).trim()) return res.status(400).json({ error: "Add a display name." });
    await run("UPDATE users SET display_name = ? WHERE id = ?", [String(display_name).trim().slice(0, 60), u.id]);
  }
  if (active !== void 0 && u.role === "assistant") {
    await run("UPDATE users SET active = ? WHERE id = ?", [active ? 1 : 0, u.id]);
    if (!active) await destroyUserSessions(u.id);
  }
  await broadcast("users", "metrics", "leads");
  res.json({ users: await usersPayload() });
});
app.put("/api/users/:id/credentials", requireOwner, async (req, res) => {
  const target = await get("SELECT * FROM users WHERE id = ?", [Number(req.params.id)]);
  if (!target) return res.status(404).json({ error: "User not found." });
  const { current_password, username, password } = req.body || {};
  if (!verifyPassword(String(current_password || ""), req.user.password_hash)) {
    return res.status(403).json({ error: "Your current password is incorrect." });
  }
  const updates = {};
  if (username !== void 0 && username.trim() !== target.username) {
    const e = validateUsername(username);
    if (e) return res.status(400).json({ error: e });
    if (await get("SELECT 1 AS x FROM users WHERE lower(username) = lower(?) AND id <> ?", [username.trim(), target.id])) return res.status(409).json({ error: "That username is already taken." });
    updates.username = username.trim();
  }
  if (password) {
    const e = validatePassword(password);
    if (e) return res.status(400).json({ error: e });
    if (verifyPassword(password, target.password_hash)) return res.status(400).json({ error: "Choose a password that\u2019s different from the current one." });
    updates.password_hash = hashPassword(password);
    updates.must_change = 0;
    updates.password_changed_at = now();
  }
  if (!Object.keys(updates).length) return res.status(400).json({ error: "Nothing to update." });
  const sets = Object.keys(updates).map((k) => `${k} = ?`).join(", ");
  await run(`UPDATE users SET ${sets} WHERE id = ?`, [...Object.values(updates), target.id]);
  if (updates.password_hash) await destroyUserSessions(target.id, target.id === req.user.id ? req.token : null);
  await broadcast("users");
  res.json({ users: await usersPayload() });
});
app.get("/api/settings", requireOwner, requireUser, async (req, res) => {
  res.json({
    timezone: await timezone(),
    templates: { ...DEFAULT_TEMPLATES, ...await getSetting("templates", {}) },
    default_templates: DEFAULT_TEMPLATES,
    default_targets: { ...DEFAULT_TARGETS, ...await defaultTargets() }
  });
});
app.put("/api/settings", requireOwner, requireUser, async (req, res) => {
  const { timezone: tz, templates, regenerate_notes } = req.body || {};
  if (tz) {
    try {
      new Intl.DateTimeFormat("en", { timeZone: tz });
    } catch {
      return res.status(400).json({ error: "Choose a valid time zone." });
    }
    await setSetting("timezone", tz);
  }
  if (templates) {
    const clean = {};
    for (const k of ["connection", "followup"]) if (typeof templates[k] === "string" && templates[k].trim()) clean[k] = templates[k].trim().slice(0, 2e3);
    await setSetting("templates", clean);
    if (regenerate_notes) {
      const t = { ...DEFAULT_TEMPLATES, ...clean };
      const rows = await all("SELECT * FROM leads WHERE status IN ('New','Assigned','Profile opened')");
      await tx(async () => {
        for (const l of rows) await run("UPDATE leads SET connection_note = ?, followup_note = ?, updated_at = ? WHERE id = ?", [renderTemplate(t.connection, l), renderTemplate(t.followup, l), now(), l.id]);
      });
    }
  }
  await broadcast("settings", "metrics", "leads");
  res.json({ ok: true });
});
var upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024, files: 1 } });
function fixName(n) {
  if (/[^\x00-\xff]/.test(n)) return n;
  const d = Buffer.from(n, "latin1").toString("utf8");
  return d.includes("\uFFFD") ? n : d;
}
async function importFor(req) {
  const row = await get("SELECT * FROM imports WHERE id = ?", [Number(req.params.id)]);
  if (!row || req.user.role !== "owner" && row.user_id !== req.user.id) throw httpError(404, "Import not found.");
  return row;
}
function importPayload(row, names) {
  const parsed = row.parsed ? JSON.parse(row.parsed) : null;
  let preview = null;
  if (parsed?.kind === "table") {
    preview = { kind: "table", review_required: !!parsed.review_required, sheets: parsed.sheets.map((s) => ({ name: s.name, header_row: s.header_row, mapping: s.mapping, row_count: s.rows.length, rows: s.rows.slice(0, 40) })), text_candidates: parsed.text_candidates?.length || 0 };
  } else if (parsed) preview = { kind: "document", review_required: true, candidates: parsed.candidates };
  return {
    id: row.id,
    filename: row.filename,
    file_kind: row.file_kind,
    status: row.status,
    created_at: row.created_at,
    completed_at: row.completed_at,
    user_name: names[row.user_id] || "Unknown",
    summary: row.summary ? JSON.parse(row.summary) : null,
    preview
  };
}
app.post("/api/imports", requireUser, (req, res, next) => {
  upload.single("file")(req, res, async (err) => {
    try {
      if (err) {
        if (err.code === "LIMIT_FILE_SIZE") return res.status(400).json({ error: `That file is larger than ${MAX_UPLOAD_MB} MB. Split it into smaller files and try again.` });
        return res.status(400).json({ error: "The upload didn\u2019t complete. Please try again." });
      }
      if (!req.file) return res.status(400).json({ error: "Choose a file to upload." });
      const original = path4.basename(fixName(req.file.originalname)).slice(0, 200);
      let parsed;
      try {
        parsed = await parseFile(req.file.buffer, original);
      } catch (e) {
        if (e instanceof UnsupportedFileError) return res.status(400).json({ error: e.message });
        console.error(e);
        return res.status(400).json({ error: "This file couldn\u2019t be read. Check that it isn\u2019t damaged and try again." });
      }
      if (parsed.kind === "document" && !parsed.candidates.length) {
        return res.status(400).json({ error: "No possible leads were found in this document. Records need at least a LinkedIn profile URL or an email address." });
      }
      const id = await createImport(req.user, original, parsed.file_kind, parsed);
      await broadcast("imports");
      res.json({ import: importPayload(await get("SELECT * FROM imports WHERE id = ?", [id]), await userNames()), fields: FIELD_LIST });
    } catch (e) {
      next(e);
    }
  });
});
app.get("/api/imports", requireUser, async (req, res) => {
  const rows = req.user.role === "owner" ? await all("SELECT * FROM imports ORDER BY created_at DESC LIMIT 50") : await all("SELECT * FROM imports WHERE user_id = ? ORDER BY created_at DESC LIMIT 50", [req.user.id]);
  const names = await userNames();
  res.json({ imports: rows.map((r) => {
    const p = importPayload({ ...r, parsed: null }, names);
    delete p.preview;
    return p;
  }), fields: FIELD_LIST });
});
app.get("/api/imports/:id", requireUser, async (req, res) => res.json({ import: importPayload(await importFor(req), await userNames()), fields: FIELD_LIST }));
app.post("/api/imports/:id/suggest", requireUser, async (req, res) => {
  const row = await importFor(req);
  const parsed = JSON.parse(row.parsed || "null");
  const sheet = parsed?.sheets?.[Number(req.body?.sheet) || 0];
  if (!sheet) return res.status(400).json({ error: "Choose a sheet." });
  res.json({ mapping: suggestMapping(sheet.rows[Number(req.body?.header_row) || 0] || []) });
});
app.post("/api/imports/:id/validate", requireUser, async (req, res) => {
  const row = await importFor(req);
  const parsed = JSON.parse(row.parsed || "null");
  const records = applyReview(recordsForImport(row, req.body || {}), req.body || {}, !!parsed?.review_required || parsed?.kind === "document");
  const results = await validateRecords(records);
  const counts = { total: results.length, ready: 0, duplicate: 0, error: 0, warnings: 0 };
  for (const r of results) {
    counts[r.state]++;
    if (r.warnings.length) counts.warnings++;
  }
  res.json({ results: results.map((r, i) => ({ ...r, raw: stripRaw(records[i]) })), counts });
});
function stripRaw(r) {
  const o = { ...r };
  delete o._extra;
  delete o._include;
  return o;
}
app.post("/api/imports/:id/commit", requireUser, async (req, res) => {
  const row = await importFor(req);
  if (row.status !== "preview") return res.status(409).json({ error: "This import has already been completed or cancelled." });
  const parsed = JSON.parse(row.parsed || "null");
  const records = applyReview(recordsForImport(row, req.body || {}), req.body || {}, !!parsed?.review_required || parsed?.kind === "document");
  let assignTo = null;
  if (req.user.role === "owner" && req.body?.assign_to) {
    const a = await get("SELECT id FROM users WHERE id = ? AND role = 'assistant' AND active = 1", [Number(req.body.assign_to)]);
    if (!a) return res.status(400).json({ error: "Choose an active assistant." });
    assignTo = a.id;
  }
  const claimed = await run("UPDATE imports SET status = 'committing' WHERE id = ? AND status = 'preview'", [row.id]);
  if (!claimed.changes) return res.status(409).json({ error: "This import is already being added." });
  try {
    res.json({ summary: await commitImport(row, records, req.user, { assignTo }) });
  } catch (e) {
    await run("UPDATE imports SET status = 'preview' WHERE id = ? AND status = 'committing'", [row.id]);
    throw e;
  }
});
app.post("/api/imports/:id/cancel", requireUser, async (req, res) => {
  const row = await importFor(req);
  if (row.status === "preview") {
    await run("UPDATE imports SET status = 'cancelled', parsed = NULL, completed_at = ? WHERE id = ?", [now(), row.id]);
    await broadcast("imports");
  }
  res.json({ ok: true });
});
app.use("/api", (req, res) => res.status(404).json({ error: "Not found." }));
app.use((err, req, res, next) => {
  const status = err.status || (err.type === "entity.too.large" ? 413 : 500);
  if (status >= 500) console.error(err);
  res.status(status).json({ error: status >= 500 ? "Something went wrong on our side. Please try again." : err.message });
});
var app_default = app;

// api/index.js
var index_default = app_default;
export {
  index_default as default
};
