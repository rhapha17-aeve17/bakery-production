import os
import sqlite3

DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

    class PgCursor:
        def __init__(self, cur, conn):
            self._cur = cur
            self._conn = conn
            self._lastrowid = None

        def execute(self, query, params=None):
            query = query.replace('?', '%s')
            if 'last_insert_rowid()' in query:
                query = query.replace('last_insert_rowid()', 'lastval()')
            self._cur.execute(query, tuple(params) if params else None)
            if query.strip().upper().startswith('INSERT'):
                try:
                    temp = self._conn.cursor()
                    temp.execute("SELECT lastval()")
                    self._lastrowid = temp.fetchone()['lastval']
                    temp.close()
                except Exception:
                    self._lastrowid = None
            return self

        @property
        def lastrowid(self):
            return self._lastrowid

        def fetchone(self):
            return self._cur.fetchone()

        def fetchall(self):
            return self._cur.fetchall()

    class PgConnection:
        def __init__(self):
            self._conn = psycopg2.connect(
                DATABASE_URL,
                cursor_factory=psycopg2.extras.RealDictCursor
            )

        def execute(self, query, params=None):
            cur = PgCursor(self._conn.cursor(), self._conn)
            cur.execute(query, params)
            return cur

        def cursor(self):
            return PgCursor(self._conn.cursor(), self._conn)

        def commit(self):
            self._conn.commit()

        def close(self):
            self._conn.close()


def get_db():
    if DATABASE_URL:
        return PgConnection()
    else:
        conn = sqlite3.connect('production.db')
        conn.row_factory = sqlite3.Row
        return conn


def safe_add_column(c, table, column, col_type):
    if DATABASE_URL:
        c.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}')
    else:
        try:
            c.execute(f'ALTER TABLE {table} ADD COLUMN {column} {col_type}')
        except Exception:
            pass


def init_db():
    conn = get_db()
    c = conn.cursor()
    auto_id = "SERIAL PRIMARY KEY" if DATABASE_URL else "INTEGER PRIMARY KEY AUTOINCREMENT"

    c.execute(f'''CREATE TABLE IF NOT EXISTS products
                 (id {auto_id},
                  name TEXT NOT NULL UNIQUE,
                  unit TEXT NOT NULL,
                  price REAL DEFAULT 0,
                  cost REAL DEFAULT 0,
                  stock_type TEXT DEFAULT '일반',
                  category TEXT DEFAULT '기타',
                  ecount_code TEXT,
                  display_order INTEGER DEFAULT 999,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS production_records
                 (id {auto_id},
                  product_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  production_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS materials
                 (id {auto_id},
                  name TEXT NOT NULL UNIQUE,
                  type TEXT DEFAULT '원자재',
                  weight REAL NOT NULL,
                  unit TEXT DEFAULT 'g',
                  purchase_price REAL DEFAULT 0,
                  price_per_gram REAL DEFAULT 0,
                  price_per_unit REAL DEFAULT 0,
                  supplier TEXT,
                  note TEXT,
                  ecount_code TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS product_materials
                 (id {auto_id},
                  product_id INTEGER NOT NULL,
                  material_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS target_production
                 (id {auto_id},
                  product_id INTEGER NOT NULL UNIQUE,
                  weekday_target REAL DEFAULT 0,
                  weekend_target REAL DEFAULT 0,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS material_recipes
                 (id {auto_id},
                  prep_material_id INTEGER NOT NULL,
                  ingredient_material_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS inventory_records
                 (id {auto_id},
                  product_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  inventory_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS irregular_product_records
                 (id {auto_id},
                  product_id INTEGER NOT NULL,
                  opening_inventory REAL NOT NULL DEFAULT 0,
                  production REAL NOT NULL DEFAULT 0,
                  donation REAL NOT NULL DEFAULT 0,
                  closing_inventory REAL NOT NULL DEFAULT 0,
                  record_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS sales_records
                 (id {auto_id},
                  product_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  sales_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS material_receipts
                 (id {auto_id},
                  material_id INTEGER NOT NULL,
                  receipt_date DATE NOT NULL,
                  quantity REAL NOT NULL,
                  unit_price REAL NOT NULL,
                  supplier TEXT,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS ecount_settings
                 (id {auto_id},
                  com_code TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  zone TEXT NOT NULL,
                  api_cert_key TEXT NOT NULL,
                  lan_type TEXT DEFAULT 'ko-KR',
                  is_active INTEGER DEFAULT 1,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    c.execute(f'''CREATE TABLE IF NOT EXISTS ecount_sync_logs
                 (id {auto_id},
                  sync_type TEXT NOT NULL,
                  record_id INTEGER,
                  record_type TEXT,
                  status TEXT NOT NULL,
                  request_data TEXT,
                  response_data TEXT,
                  error_message TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    safe_add_column(c, 'products', 'ecount_code', 'TEXT')
    safe_add_column(c, 'products', 'price', 'REAL DEFAULT 0')
    safe_add_column(c, 'products', 'cost', 'REAL DEFAULT 0')
    safe_add_column(c, 'products', 'stock_type', "TEXT DEFAULT '일반'")
    safe_add_column(c, 'products', 'category', "TEXT DEFAULT '기타'")
    safe_add_column(c, 'products', 'display_order', 'INTEGER DEFAULT 999')
    safe_add_column(c, 'materials', 'price_per_unit', 'REAL DEFAULT 0')
    safe_add_column(c, 'materials', 'ecount_code', 'TEXT')

    conn.commit()
    conn.close()
