from flask import Flask, render_template, request, jsonify
import sqlite3
from datetime import datetime, timedelta
import os
import requests
import json

app = Flask(__name__)

# 데이터베이스 초기화
def init_db():
    conn = sqlite3.connect('production.db')
    c = conn.cursor()

    # 제품 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS products
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL UNIQUE,
                  unit TEXT NOT NULL,
                  price REAL DEFAULT 0,
                  cost REAL DEFAULT 0,
                  stock_type TEXT DEFAULT '일반',
                  category TEXT DEFAULT '기타',
                  ecount_code TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # ecount_code 컬럼 추가 (기존 DB용)
    try:
        c.execute('ALTER TABLE products ADD COLUMN ecount_code TEXT')
    except:
        pass

    # 생산량 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS production_records
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  product_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  production_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (product_id) REFERENCES products (id))''')

    # 자재 테이블 (원자재, 부자재)
    c.execute('''CREATE TABLE IF NOT EXISTS materials
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    # price_per_unit 컬럼 추가 (기존 DB용)
    try:
        c.execute('ALTER TABLE materials ADD COLUMN price_per_unit REAL DEFAULT 0')
    except:
        pass

    # ecount_code 컬럼 추가 (기존 DB용)
    try:
        c.execute('ALTER TABLE materials ADD COLUMN ecount_code TEXT')
    except:
        pass

    # 제품-자재 관계 테이블 (레시피/BOM)
    c.execute('''CREATE TABLE IF NOT EXISTS product_materials
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  product_id INTEGER NOT NULL,
                  material_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE,
                  FOREIGN KEY (material_id) REFERENCES materials (id) ON DELETE CASCADE)''')

    # 목표 생산량 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS target_production
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  product_id INTEGER NOT NULL UNIQUE,
                  weekday_target REAL DEFAULT 0,
                  weekend_target REAL DEFAULT 0,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE)''')

    # 자재 레시피 테이블 (프랩 자재의 원재료 구성)
    c.execute('''CREATE TABLE IF NOT EXISTS material_recipes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  prep_material_id INTEGER NOT NULL,
                  ingredient_material_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (prep_material_id) REFERENCES materials (id) ON DELETE CASCADE,
                  FOREIGN KEY (ingredient_material_id) REFERENCES materials (id) ON DELETE CASCADE)''')

    # 재고 관리 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS inventory_records
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  product_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  inventory_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE)''')

    # 비정기 제품 관리 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS irregular_product_records
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  product_id INTEGER NOT NULL,
                  opening_inventory REAL NOT NULL DEFAULT 0,
                  production REAL NOT NULL DEFAULT 0,
                  donation REAL NOT NULL DEFAULT 0,
                  closing_inventory REAL NOT NULL DEFAULT 0,
                  record_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE)''')

    # 판매 기록 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS sales_records
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  product_id INTEGER NOT NULL,
                  quantity REAL NOT NULL,
                  sales_date DATE NOT NULL,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (product_id) REFERENCES products (id) ON DELETE CASCADE)''')

    # 자재 입고 이력 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS material_receipts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  material_id INTEGER NOT NULL,
                  receipt_date DATE NOT NULL,
                  quantity REAL NOT NULL,
                  unit_price REAL NOT NULL,
                  supplier TEXT,
                  note TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY (material_id) REFERENCES materials (id) ON DELETE CASCADE)''')

    # 기존 테이블에 새 컬럼 추가 (이미 있으면 에러 무시)
    try:
        c.execute('ALTER TABLE products ADD COLUMN price REAL DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE products ADD COLUMN cost REAL DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE products ADD COLUMN stock_type TEXT DEFAULT "일반"')
    except:
        pass
    try:
        c.execute('ALTER TABLE products ADD COLUMN category TEXT DEFAULT "기타"')
    except:
        pass
    try:
        c.execute('ALTER TABLE products ADD COLUMN display_order INTEGER DEFAULT 999')
    except:
        pass

    # Ecount 설정 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS ecount_settings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  com_code TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  zone TEXT NOT NULL,
                  api_cert_key TEXT NOT NULL,
                  lan_type TEXT DEFAULT 'ko-KR',
                  is_active INTEGER DEFAULT 1,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # Ecount 동기화 이력 테이블
    c.execute('''CREATE TABLE IF NOT EXISTS ecount_sync_logs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  sync_type TEXT NOT NULL,
                  record_id INTEGER,
                  record_type TEXT,
                  status TEXT NOT NULL,
                  request_data TEXT,
                  response_data TEXT,
                  error_message TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()

# 데이터베이스 연결 함수
def get_db():
    conn = sqlite3.connect('production.db')
    conn.row_factory = sqlite3.Row
    return conn

# ===== Ecount API 함수 =====

# Ecount API 로그인
def ecount_login(settings):
    """Ecount API에 로그인하여 SESSION_ID를 반환합니다."""
    try:
        url = f"https://sboapi{settings['zone']}.ecount.com/OAPI/V2/OAPILogin"

        payload = {
            "COM_CODE": settings['com_code'],
            "USER_ID": settings['user_id'],
            "ZONE": settings['zone'],
            "API_CERT_KEY": settings['api_cert_key'],
            "LAN_TYPE": settings.get('lan_type', 'ko-KR')
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()

        result = response.json()

        if result.get('SESSION_ID'):
            return {'success': True, 'session_id': result['SESSION_ID'], 'data': result}
        else:
            return {'success': False, 'error': '로그인 실패: SESSION_ID를 받지 못했습니다.', 'data': result}

    except requests.exceptions.RequestException as e:
        return {'success': False, 'error': f'API 요청 실패: {str(e)}'}
    except Exception as e:
        return {'success': False, 'error': f'예상치 못한 오류: {str(e)}'}

# Ecount 설정 가져오기
def get_ecount_settings():
    """활성화된 Ecount 설정을 가져옵니다."""
    conn = get_db()
    settings = conn.execute('SELECT * FROM ecount_settings WHERE is_active = 1 ORDER BY created_at DESC LIMIT 1').fetchone()
    conn.close()
    return dict(settings) if settings else None

# Ecount 동기화 로그 기록
def log_ecount_sync(sync_type, record_id, record_type, status, request_data=None, response_data=None, error_message=None):
    """Ecount 동기화 로그를 기록합니다."""
    conn = get_db()
    conn.execute('''INSERT INTO ecount_sync_logs
                    (sync_type, record_id, record_type, status, request_data, response_data, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                 (sync_type, record_id, record_type, status,
                  json.dumps(request_data, ensure_ascii=False) if request_data else None,
                  json.dumps(response_data, ensure_ascii=False) if response_data else None,
                  error_message))
    conn.commit()
    conn.close()

# Ecount에 판매 데이터 전송 (생산 실적 → 판매)
def sync_production_to_ecount_sale(production_record_id):
    """생산 실적을 Ecount 판매 데이터로 전송합니다."""
    settings = get_ecount_settings()
    if not settings:
        return {'success': False, 'error': 'Ecount 설정이 없습니다.'}

    # 로그인
    login_result = ecount_login(settings)
    if not login_result['success']:
        log_ecount_sync('sale', production_record_id, 'production', 'failed', None, None, login_result['error'])
        return login_result

    session_id = login_result['session_id']

    # 생산 실적 데이터 가져오기
    conn = get_db()
    record = conn.execute('''SELECT pr.*, p.name as product_name, p.price, p.ecount_code
                             FROM production_records pr
                             JOIN products p ON pr.product_id = p.id
                             WHERE pr.id = ?''', (production_record_id,)).fetchone()
    conn.close()

    if not record:
        return {'success': False, 'error': '생산 실적을 찾을 수 없습니다.'}

    # ecount_code 검증
    if not record['ecount_code']:
        error_msg = f'제품 "{record["product_name"]}"에 이카운트 제품코드가 설정되지 않았습니다.'
        log_ecount_sync('sale', production_record_id, 'production', 'failed', None, None, error_msg)
        return {'success': False, 'error': error_msg}

    try:
        url = f"https://sboapi{settings['zone']}.ecount.com/OAPI/V2/Sale/SaveSale?SESSION_ID={session_id}"

        payload = {
            "SalesList": {
                "BulkDatas": [{
                    "PROD_CD": record['ecount_code'],
                    "PROD_DES": record['product_name'],
                    "QTY": record['quantity'],
                    "UNIT_AMT": record['price'],
                    "SALE_DATE": record['production_date'],
                    "Line": 1
                }]
            }
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()

        result = response.json()

        log_ecount_sync('sale', production_record_id, 'production', 'success', payload, result, None)

        return {'success': True, 'data': result}

    except requests.exceptions.RequestException as e:
        error_msg = f'API 요청 실패: {str(e)}'
        log_ecount_sync('sale', production_record_id, 'production', 'failed', payload, None, error_msg)
        return {'success': False, 'error': error_msg}
    except Exception as e:
        error_msg = f'예상치 못한 오류: {str(e)}'
        log_ecount_sync('sale', production_record_id, 'production', 'failed', payload, None, error_msg)
        return {'success': False, 'error': error_msg}

# Ecount에 매입 데이터 전송 (자재 입고 → 매입)
def sync_receipt_to_ecount_purchase(receipt_id):
    """자재 입고를 Ecount 매입 데이터로 전송합니다."""
    settings = get_ecount_settings()
    if not settings:
        return {'success': False, 'error': 'Ecount 설정이 없습니다.'}

    # 로그인
    login_result = ecount_login(settings)
    if not login_result['success']:
        log_ecount_sync('purchase', receipt_id, 'receipt', 'failed', None, None, login_result['error'])
        return login_result

    session_id = login_result['session_id']

    # 입고 데이터 가져오기
    conn = get_db()
    record = conn.execute('''SELECT mr.*, m.name as material_name, m.ecount_code
                             FROM material_receipts mr
                             JOIN materials m ON mr.material_id = m.id
                             WHERE mr.id = ?''', (receipt_id,)).fetchone()
    conn.close()

    if not record:
        return {'success': False, 'error': '입고 기록을 찾을 수 없습니다.'}

    # ecount_code 검증
    if not record['ecount_code']:
        error_msg = f'자재 "{record["material_name"]}"에 이카운트 제품코드가 설정되지 않았습니다.'
        log_ecount_sync('purchase', receipt_id, 'receipt', 'failed', None, None, error_msg)
        return {'success': False, 'error': error_msg}

    try:
        url = f"https://sboapi{settings['zone']}.ecount.com/OAPI/V2/Purchases/SavePurchases?SESSION_ID={session_id}"

        payload = {
            "PurchasesList": {
                "BulkDatas": [{
                    "PROD_CD": record['ecount_code'],
                    "PROD_DES": record['material_name'],
                    "QTY": record['quantity'],
                    "UNIT_AMT": record['unit_price'],
                    "PURCH_DATE": record['receipt_date'],
                    "SUPPLIER": record['supplier'] or '',
                    "Line": 1
                }]
            }
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()

        result = response.json()

        log_ecount_sync('purchase', receipt_id, 'receipt', 'success', payload, result, None)

        return {'success': True, 'data': result}

    except requests.exceptions.RequestException as e:
        error_msg = f'API 요청 실패: {str(e)}'
        log_ecount_sync('purchase', receipt_id, 'receipt', 'failed', payload, None, error_msg)
        return {'success': False, 'error': error_msg}
    except Exception as e:
        error_msg = f'예상치 못한 오류: {str(e)}'
        log_ecount_sync('purchase', receipt_id, 'receipt', 'failed', payload, None, error_msg)
        return {'success': False, 'error': error_msg}

# 메인 페이지
@app.route('/')
def index():
    return render_template('index.html')

# 제품 목록 조회
@app.route('/api/products', methods=['GET'])
def get_products():
    conn = get_db()
    products = conn.execute('SELECT * FROM products ORDER BY display_order, name').fetchall()
    conn.close()
    return jsonify([dict(p) for p in products])

# 제품 추가
@app.route('/api/products', methods=['POST'])
def add_product():
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO products
                     (name, unit, price, cost, stock_type, category, ecount_code, display_order)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (data['name'], data['unit'],
                   data.get('price', 0), data.get('cost', 0),
                   data.get('stock_type', '일반'), data.get('category', '기타'),
                   data.get('ecount_code'),
                   data.get('display_order', 999)))
        conn.commit()
        product_id = c.lastrowid
        conn.close()
        return jsonify({'success': True, 'id': product_id})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': '이미 존재하는 제품명입니다'}), 400

# 제품 수정
@app.route('/api/products/<int:product_id>', methods=['PUT'])
def update_product(product_id):
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''UPDATE products
                     SET name=?, unit=?, price=?, cost=?, stock_type=?, category=?, ecount_code=?, display_order=?
                     WHERE id=?''',
                  (data['name'], data['unit'],
                   data.get('price', 0), data.get('cost', 0),
                   data.get('stock_type', '일반'), data.get('category', '기타'),
                   data.get('ecount_code'),
                   data.get('display_order', 999),
                   product_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': '이미 존재하는 제품명입니다'}), 400

# 제품 삭제
@app.route('/api/products/<int:product_id>', methods=['DELETE'])
def delete_product(product_id):
    conn = get_db()
    conn.execute('DELETE FROM production_records WHERE product_id = ?', (product_id,))
    conn.execute('DELETE FROM products WHERE id = ?', (product_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

# 생산량 기록 추가
@app.route('/api/production', methods=['POST'])
def add_production():
    data = request.json
    conn = get_db()
    c = conn.cursor()
    c.execute('''INSERT INTO production_records
                 (product_id, quantity, production_date, note)
                 VALUES (?, ?, ?, ?)''',
              (data['product_id'], data['quantity'],
               data['production_date'], data.get('note', '')))
    conn.commit()
    record_id = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': record_id})

# 생산량 기록 조회
@app.route('/api/production', methods=['GET'])
def get_production():
    product_id = request.args.get('product_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    conn = get_db()
    query = '''SELECT pr.*, p.name as product_name, p.unit
               FROM production_records pr
               JOIN products p ON pr.product_id = p.id
               WHERE 1=1'''
    params = []

    if product_id:
        query += ' AND pr.product_id = ?'
        params.append(product_id)

    if start_date:
        query += ' AND pr.production_date >= ?'
        params.append(start_date)

    if end_date:
        query += ' AND pr.production_date <= ?'
        params.append(end_date)

    query += ' ORDER BY pr.production_date DESC, pr.created_at DESC'

    records = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in records])

# 생산량 기록 삭제
@app.route('/api/production/<int:record_id>', methods=['DELETE'])
def delete_production(record_id):
    conn = get_db()
    conn.execute('DELETE FROM production_records WHERE id = ?', (record_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

# 통계 조회 (매출, 원가, 이익 포함)
@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category')

    conn = get_db()
    query = '''SELECT p.id, p.name, p.unit, p.price, p.cost, p.category, p.stock_type,
                      SUM(pr.quantity) as total_quantity,
                      COUNT(pr.id) as record_count,
                      SUM(pr.quantity * p.price) as total_sales,
                      SUM(pr.quantity * p.cost) as total_cost,
                      SUM(pr.quantity * (p.price - p.cost)) as total_profit
               FROM products p
               LEFT JOIN production_records pr ON p.id = pr.product_id'''
    params = []
    conditions = []

    if start_date:
        conditions.append('pr.production_date >= ?')
        params.append(start_date)
    if end_date:
        conditions.append('pr.production_date <= ?')
        params.append(end_date)
    if category:
        conditions.append('p.category = ?')
        params.append(category)

    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    query += ' GROUP BY p.id, p.name, p.unit, p.price, p.cost, p.category, p.stock_type ORDER BY p.display_order, p.name'

    stats = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(s) for s in stats])

# 전체 요약 통계
@app.route('/api/statistics/summary', methods=['GET'])
def get_statistics_summary():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    conn = get_db()
    query = '''SELECT
                   COUNT(DISTINCT p.id) as product_count,
                   SUM(pr.quantity) as total_quantity,
                   SUM(pr.quantity * p.price) as total_sales,
                   SUM(pr.quantity * p.cost) as total_cost,
                   SUM(pr.quantity * (p.price - p.cost)) as total_profit
               FROM products p
               LEFT JOIN production_records pr ON p.id = pr.product_id'''
    params = []
    conditions = []

    if start_date:
        conditions.append('pr.production_date >= ?')
        params.append(start_date)
    if end_date:
        conditions.append('pr.production_date <= ?')
        params.append(end_date)

    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)

    result = conn.execute(query, params).fetchone()

    # 카테고리별 통계
    cat_query = '''SELECT p.category,
                          COUNT(DISTINCT p.id) as product_count,
                          SUM(pr.quantity) as total_quantity,
                          SUM(pr.quantity * p.price) as total_sales,
                          SUM(pr.quantity * p.cost) as total_cost,
                          SUM(pr.quantity * (p.price - p.cost)) as total_profit
                   FROM products p
                   LEFT JOIN production_records pr ON p.id = pr.product_id'''

    if conditions:
        cat_query += ' WHERE ' + ' AND '.join(conditions)

    cat_query += ' GROUP BY p.category ORDER BY p.category'

    categories = conn.execute(cat_query, params).fetchall()
    conn.close()

    return jsonify({
        'summary': dict(result) if result else {},
        'by_category': [dict(c) for c in categories]
    })

# 특정 날짜의 모든 제품 생산량 조회 (엑셀 스타일 입력용)
@app.route('/api/production/grid', methods=['GET'])
def get_production_grid():
    production_date = request.args.get('date')

    conn = get_db()
    # 정기 제품과 해당 날짜의 생산량을 함께 조회 (비정기 제품 제외)
    query = '''SELECT p.id, p.name, p.unit, p.category, p.price, p.cost,
                      pr.quantity, pr.id as record_id
               FROM products p
               LEFT JOIN production_records pr
                   ON p.id = pr.product_id AND pr.production_date = ?
               WHERE p.category != '비정기 제품'
               ORDER BY p.display_order, p.name'''

    results = conn.execute(query, [production_date]).fetchall()
    conn.close()
    return jsonify([dict(r) for r in results])

# 일괄 생산량 저장/수정 (엑셀 스타일 입력용)
@app.route('/api/production/bulk', methods=['POST'])
def bulk_save_production():
    data = request.json
    production_date = data['date']
    products = data['products']  # [{product_id, quantity}, ...]

    conn = get_db()
    c = conn.cursor()

    try:
        for item in products:
            product_id = item['product_id']
            quantity = item.get('quantity')

            # 빈 값이거나 0이면 기존 레코드 삭제
            if quantity is None or quantity == '' or float(quantity) == 0:
                c.execute('''DELETE FROM production_records
                            WHERE product_id = ? AND production_date = ?''',
                         (product_id, production_date))
            else:
                # 해당 날짜의 레코드가 있는지 확인
                existing = c.execute('''SELECT id FROM production_records
                                       WHERE product_id = ? AND production_date = ?''',
                                    (product_id, production_date)).fetchone()

                if existing:
                    # 업데이트
                    c.execute('''UPDATE production_records
                                SET quantity = ?
                                WHERE product_id = ? AND production_date = ?''',
                             (quantity, product_id, production_date))
                else:
                    # 새로 추가
                    c.execute('''INSERT INTO production_records
                                (product_id, quantity, production_date, note)
                                VALUES (?, ?, ?, ?)''',
                             (product_id, quantity, production_date, ''))

        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# ==================== 자재 관리 API ====================

# 자재 목록 조회
@app.route('/api/materials', methods=['GET'])
def get_materials():
    conn = get_db()
    materials = conn.execute('SELECT * FROM materials ORDER BY type, name').fetchall()

    # 프랩 자재의 원가를 레시피 기반으로 계산
    result = []
    for material in materials:
        m_dict = dict(material)

        # 프랩 자재인 경우 레시피 기반 원가 계산
        if m_dict['type'] == '프랩':
            recipe = conn.execute('''SELECT mr.quantity, ing.price_per_unit
                                    FROM material_recipes mr
                                    JOIN materials ing ON mr.ingredient_material_id = ing.id
                                    WHERE mr.prep_material_id = ?''', (m_dict['id'],)).fetchall()

            calculated_cost = sum(r['quantity'] * (r['price_per_unit'] or 0) for r in recipe)

            # 계산된 원가를 추가 필드로 제공
            m_dict['recipe_cost'] = calculated_cost

            # 단위당 가격도 계산
            if calculated_cost > 0 and m_dict['weight'] > 0:
                m_dict['recipe_price_per_unit'] = calculated_cost / m_dict['weight']

        result.append(m_dict)

    conn.close()
    return jsonify(result)

# 자재 추가
@app.route('/api/materials', methods=['POST'])
def add_material():
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()

        # 단위당 가격 자동 계산
        quantity = float(data['weight'])  # weight 필드명 유지 (하위 호환성)
        material_type = data.get('type', '원자재')

        # 프랩 자재는 입고 단가를 0으로 설정 (레시피에서 자동 계산)
        if material_type == '프랩':
            purchase_price = 0
            price_per_unit = 0
            price_per_gram = 0
        else:
            purchase_price = float(data.get('purchase_price', 0))
            price_per_unit = purchase_price / quantity if quantity > 0 else 0
            price_per_gram = price_per_unit  # 하위 호환성

        c.execute('''INSERT INTO materials
                     (name, type, weight, unit, purchase_price, price_per_gram, price_per_unit, supplier, note, ecount_code)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (data['name'], material_type,
                   quantity, data.get('unit', 'g'),
                   purchase_price, price_per_gram, price_per_unit,
                   data.get('supplier', ''), data.get('note', ''),
                   data.get('ecount_code')))
        conn.commit()
        material_id = c.lastrowid
        conn.close()
        return jsonify({'success': True, 'id': material_id})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': '이미 존재하는 자재명입니다'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# 자재 수정
@app.route('/api/materials/<int:material_id>', methods=['PUT'])
def update_material(material_id):
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()

        # 단위당 가격 자동 계산
        quantity = float(data['weight'])
        material_type = data.get('type', '원자재')

        # 프랩 자재는 입고 단가를 레시피에서 계산
        if material_type == '프랩':
            # 레시피 기반 원가 계산
            recipe = conn.execute('''SELECT mr.quantity, ing.price_per_unit
                                    FROM material_recipes mr
                                    JOIN materials ing ON mr.ingredient_material_id = ing.id
                                    WHERE mr.prep_material_id = ?''', (material_id,)).fetchall()
            purchase_price = sum(r['quantity'] * (r['price_per_unit'] or 0) for r in recipe)
            price_per_unit = purchase_price / quantity if quantity > 0 else 0
            price_per_gram = price_per_unit
        else:
            purchase_price = float(data.get('purchase_price', 0))
            price_per_unit = purchase_price / quantity if quantity > 0 else 0
            price_per_gram = price_per_unit  # 하위 호환성

        c.execute('''UPDATE materials
                     SET name=?, type=?, weight=?, unit=?, purchase_price=?,
                         price_per_gram=?, price_per_unit=?, supplier=?, note=?, ecount_code=?
                     WHERE id=?''',
                  (data['name'], material_type,
                   quantity, data.get('unit', 'g'),
                   purchase_price, price_per_gram, price_per_unit,
                   data.get('supplier', ''), data.get('note', ''),
                   data.get('ecount_code'),
                   material_id))
        conn.commit()

        # 이 자재를 사용하는 모든 제품의 원가 재계산
        products = c.execute('''SELECT DISTINCT product_id
                               FROM product_materials
                               WHERE material_id = ?''', (material_id,)).fetchall()
        for p in products:
            update_product_cost(p['product_id'], conn)

        conn.close()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': '이미 존재하는 자재명입니다'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# 자재 삭제
@app.route('/api/materials/<int:material_id>', methods=['DELETE'])
def delete_material(material_id):
    conn = get_db()
    conn.execute('DELETE FROM product_materials WHERE material_id = ?', (material_id,))
    conn.execute('DELETE FROM material_recipes WHERE prep_material_id = ? OR ingredient_material_id = ?',
                 (material_id, material_id))
    conn.execute('DELETE FROM materials WHERE id = ?', (material_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

# ==================== 자재 레시피 API (프랩 자재용) ====================

# 자재의 레시피 조회 (프랩 자재가 어떤 재료로 만들어지는지)
@app.route('/api/materials/<int:material_id>/recipe', methods=['GET'])
def get_material_recipe(material_id):
    conn = get_db()
    # 해당 자재 정보
    material = conn.execute('SELECT * FROM materials WHERE id = ?', (material_id,)).fetchone()

    # 레시피 (재료 목록)
    recipe = conn.execute('''SELECT mr.id, mr.quantity, m.id as material_id, m.name, m.type, m.unit,
                                   m.weight, m.purchase_price, m.price_per_unit
                            FROM material_recipes mr
                            JOIN materials m ON mr.ingredient_material_id = m.id
                            WHERE mr.prep_material_id = ?
                            ORDER BY m.name''', (material_id,)).fetchall()

    conn.close()
    return jsonify({
        'material': dict(material) if material else None,
        'recipe': [dict(r) for r in recipe]
    })

# 자재 레시피에 재료 추가
@app.route('/api/materials/<int:material_id>/recipe', methods=['POST'])
def add_ingredient_to_material(material_id):
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO material_recipes (prep_material_id, ingredient_material_id, quantity)
                       VALUES (?, ?, ?)''',
                    (material_id, data['material_id'], data['quantity']))
        conn.commit()
        new_id = c.execute('SELECT last_insert_rowid()').fetchone()[0]

        # 프랩 자재의 입고 단가 재계산
        update_prep_material_cost(material_id, conn)

        conn.close()
        return jsonify({'success': True, 'id': new_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# 자재 레시피에서 재료 삭제
@app.route('/api/materials/recipe/<int:recipe_id>', methods=['DELETE'])
def delete_ingredient_from_material(recipe_id):
    conn = get_db()
    c = conn.cursor()

    # 삭제 전에 prep_material_id 가져오기
    recipe = c.execute('SELECT prep_material_id FROM material_recipes WHERE id = ?', (recipe_id,)).fetchone()

    c.execute('DELETE FROM material_recipes WHERE id = ?', (recipe_id,))
    conn.commit()

    # 프랩 자재의 입고 단가 재계산
    if recipe:
        update_prep_material_cost(recipe['prep_material_id'], conn)

    conn.close()
    return jsonify({'success': True})

# ==================== 제품 레시피 (BOM) API ====================

# 제품의 레시피 조회
@app.route('/api/products/<int:product_id>/recipe', methods=['GET'])
def get_product_recipe(product_id):
    conn = get_db()
    recipe = conn.execute('''SELECT pm.id, pm.quantity, m.*
                            FROM product_materials pm
                            JOIN materials m ON pm.material_id = m.id
                            WHERE pm.product_id = ?
                            ORDER BY m.name''', (product_id,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in recipe])

# 제품에 자재 추가
@app.route('/api/products/<int:product_id>/recipe', methods=['POST'])
def add_material_to_product(product_id):
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO product_materials (product_id, material_id, quantity)
                     VALUES (?, ?, ?)''',
                  (product_id, data['material_id'], data['quantity']))
        conn.commit()

        # 원가 재계산
        update_product_cost(product_id, conn)

        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# 레시피 항목 수정
@app.route('/api/products/<int:product_id>/recipe/<int:recipe_id>', methods=['PUT'])
def update_recipe_item(product_id, recipe_id):
    data = request.json
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute('''UPDATE product_materials SET quantity = ? WHERE id = ?''',
                  (data['quantity'], recipe_id))
        conn.commit()

        # 원가 재계산
        update_product_cost(product_id, conn)

        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# 레시피 항목 삭제
@app.route('/api/products/<int:product_id>/recipe/<int:recipe_id>', methods=['DELETE'])
def delete_recipe_item(product_id, recipe_id):
    conn = get_db()
    conn.execute('DELETE FROM product_materials WHERE id = ?', (recipe_id,))
    conn.commit()

    # 원가 재계산
    update_product_cost(product_id, conn)

    conn.close()
    return jsonify({'success': True})

# 제품 원가 자동 계산 및 업데이트 함수
def update_product_cost(product_id, conn=None):
    should_close = False
    if conn is None:
        conn = get_db()
        should_close = True

    c = conn.cursor()

    # 제품의 모든 자재 비용 합계 계산 (price_per_unit 사용, 없으면 price_per_gram)
    result = c.execute('''SELECT SUM(pm.quantity * COALESCE(m.price_per_unit, m.price_per_gram)) as total_cost
                         FROM product_materials pm
                         JOIN materials m ON pm.material_id = m.id
                         WHERE pm.product_id = ?''', (product_id,)).fetchone()

    total_cost = result['total_cost'] if result['total_cost'] else 0

    # 제품의 원가 업데이트
    c.execute('UPDATE products SET cost = ? WHERE id = ?', (total_cost, product_id))
    conn.commit()

    if should_close:
        conn.close()

    return total_cost

# 프랩 자재 원가 자동 계산 및 업데이트 함수
def update_prep_material_cost(material_id, conn=None):
    should_close = False
    if conn is None:
        conn = get_db()
        should_close = True

    c = conn.cursor()

    # 자재 정보 가져오기
    material = c.execute('SELECT weight FROM materials WHERE id = ?', (material_id,)).fetchone()
    if not material:
        if should_close:
            conn.close()
        return 0

    # 레시피 기반 원가 계산
    recipe = c.execute('''SELECT mr.quantity, ing.price_per_unit
                         FROM material_recipes mr
                         JOIN materials ing ON mr.ingredient_material_id = ing.id
                         WHERE mr.prep_material_id = ?''', (material_id,)).fetchall()

    total_cost = sum(r['quantity'] * (r['price_per_unit'] or 0) for r in recipe)

    # 단위당 가격 계산
    weight = material['weight']
    price_per_unit = total_cost / weight if weight > 0 else 0

    # 프랩 자재의 입고 단가 및 단위당 가격 업데이트
    c.execute('''UPDATE materials
                SET purchase_price = ?, price_per_unit = ?, price_per_gram = ?
                WHERE id = ?''',
             (total_cost, price_per_unit, price_per_unit, material_id))
    conn.commit()

    # 이 프랩 자재를 사용하는 제품들의 원가도 재계산
    products = c.execute('''SELECT DISTINCT product_id
                           FROM product_materials
                           WHERE material_id = ?''', (material_id,)).fetchall()
    for p in products:
        update_product_cost(p['product_id'], conn)

    if should_close:
        conn.close()

    return total_cost

# 제품 정보 조회 (레시피 포함)
@app.route('/api/products/<int:product_id>/detail', methods=['GET'])
def get_product_detail(product_id):
    conn = get_db()

    # 제품 정보
    product = conn.execute('SELECT * FROM products WHERE id = ?', (product_id,)).fetchone()

    if not product:
        conn.close()
        return jsonify({'success': False, 'error': '제품을 찾을 수 없습니다'}), 404

    # 레시피 정보
    recipe = conn.execute('''SELECT pm.id, pm.quantity, m.*,
                                    (pm.quantity * m.price_per_gram) as item_cost
                            FROM product_materials pm
                            JOIN materials m ON pm.material_id = m.id
                            WHERE pm.product_id = ?
                            ORDER BY m.name''', (product_id,)).fetchall()

    conn.close()

    return jsonify({
        'product': dict(product),
        'recipe': [dict(r) for r in recipe]
    })

# ==================== 목표 생산량 API ====================

# 목표 생산량 조회
@app.route('/api/target-production', methods=['GET'])
def get_target_production():
    conn = get_db()
    # 정기 제품과 목표 생산량을 함께 조회 (비정기 제품 제외)
    query = '''SELECT p.id, p.name, p.unit, p.category, p.price,
                      COALESCE(tp.weekday_target, 0) as weekday_target,
                      COALESCE(tp.weekend_target, 0) as weekend_target
               FROM products p
               LEFT JOIN target_production tp ON p.id = tp.product_id
               WHERE p.category != '비정기 제품'
               ORDER BY p.display_order, p.name'''

    results = conn.execute(query).fetchall()
    conn.close()
    return jsonify([dict(r) for r in results])

# 목표 생산량 일괄 저장
@app.route('/api/target-production/bulk', methods=['POST'])
def bulk_save_target_production():
    data = request.json
    targets = data['targets']  # [{product_id, weekday_target, weekend_target}, ...]

    conn = get_db()
    c = conn.cursor()

    try:
        for item in targets:
            product_id = item['product_id']
            weekday_target = item.get('weekday_target', 0)
            weekend_target = item.get('weekend_target', 0)

            # 기존 레코드가 있는지 확인
            existing = c.execute('''SELECT id FROM target_production
                                   WHERE product_id = ?''', (product_id,)).fetchone()

            if existing:
                # 업데이트
                c.execute('''UPDATE target_production
                            SET weekday_target = ?, weekend_target = ?
                            WHERE product_id = ?''',
                         (weekday_target, weekend_target, product_id))
            else:
                # 새로 추가
                c.execute('''INSERT INTO target_production
                            (product_id, weekday_target, weekend_target)
                            VALUES (?, ?, ?)''',
                         (product_id, weekday_target, weekend_target))

        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# ==================== 재고 관리 API ====================

# 특정 날짜의 모든 제품 재고량 조회
@app.route('/api/inventory/grid', methods=['GET'])
def get_inventory_grid():
    inventory_date = request.args.get('date')

    conn = get_db()
    # 정기 제품과 해당 날짜의 재고량을 함께 조회 (비정기 제품 제외)
    query = '''SELECT p.id, p.name, p.unit, p.category, p.price, p.cost, p.stock_type,
                      ir.quantity, ir.id as record_id
               FROM products p
               LEFT JOIN inventory_records ir
                   ON p.id = ir.product_id AND ir.inventory_date = ?
               WHERE p.category != '비정기 제품'
               ORDER BY p.display_order, p.name'''

    results = conn.execute(query, [inventory_date]).fetchall()
    conn.close()
    return jsonify([dict(r) for r in results])

# 일괄 재고량 저장/수정
@app.route('/api/inventory/bulk', methods=['POST'])
def bulk_save_inventory():
    data = request.json
    inventory_date = data['date']
    products = data['products']  # [{product_id, quantity}, ...]

    conn = get_db()
    c = conn.cursor()

    try:
        for item in products:
            product_id = item['product_id']
            quantity = item.get('quantity')

            # 빈 값이거나 0이면 기존 레코드 삭제
            if quantity is None or quantity == '' or float(quantity) == 0:
                c.execute('''DELETE FROM inventory_records
                            WHERE product_id = ? AND inventory_date = ?''',
                         (product_id, inventory_date))
            else:
                # 해당 날짜의 레코드가 있는지 확인
                existing = c.execute('''SELECT id FROM inventory_records
                                       WHERE product_id = ? AND inventory_date = ?''',
                                    (product_id, inventory_date)).fetchone()

                if existing:
                    # 업데이트
                    c.execute('''UPDATE inventory_records
                                SET quantity = ?
                                WHERE product_id = ? AND inventory_date = ?''',
                             (quantity, product_id, inventory_date))
                else:
                    # 새로 추가
                    c.execute('''INSERT INTO inventory_records
                                (product_id, quantity, inventory_date, note)
                                VALUES (?, ?, ?, ?)''',
                             (product_id, quantity, inventory_date, ''))

        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# 재고 이력 조회 (기간별)
@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    product_id = request.args.get('product_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    conn = get_db()
    query = '''SELECT ir.*, p.name as product_name, p.unit
               FROM inventory_records ir
               JOIN products p ON ir.product_id = p.id
               WHERE 1=1'''
    params = []

    if product_id:
        query += ' AND ir.product_id = ?'
        params.append(product_id)

    if start_date:
        query += ' AND ir.inventory_date >= ?'
        params.append(start_date)

    if end_date:
        query += ' AND ir.inventory_date <= ?'
        params.append(end_date)

    query += ' ORDER BY ir.inventory_date DESC, ir.created_at DESC'

    records = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in records])

# ==================== 비정기 제품 관리 API ====================

# 특정 날짜의 비정기 제품 데이터 조회
@app.route('/api/irregular-product/grid', methods=['GET'])
def get_irregular_product_grid():
    record_date = request.args.get('date')

    # 전날 날짜 계산
    from datetime import datetime, timedelta
    current_date = datetime.strptime(record_date, '%Y-%m-%d')
    prev_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')

    conn = get_db()
    # 비정기 제품 카테고리에 속하는 제품과 해당 날짜의 데이터, 전날 기말재고를 함께 조회
    query = '''SELECT p.id, p.name, p.unit, p.category, p.price,
                      ir.opening_inventory, ir.production, ir.donation, ir.closing_inventory,
                      ir.id as record_id,
                      prev_ir.closing_inventory as prev_closing_inventory
               FROM products p
               LEFT JOIN irregular_product_records ir
                   ON p.id = ir.product_id AND ir.record_date = ?
               LEFT JOIN irregular_product_records prev_ir
                   ON p.id = prev_ir.product_id AND prev_ir.record_date = ?
               WHERE p.category = '비정기 제품'
               ORDER BY p.display_order, p.name'''

    results = conn.execute(query, [record_date, prev_date]).fetchall()
    conn.close()
    return jsonify([dict(r) for r in results])

# 일괄 비정기 제품 데이터 저장/수정
@app.route('/api/irregular-product/bulk', methods=['POST'])
def bulk_save_irregular_product():
    data = request.json
    record_date = data['date']
    products = data['products']  # [{product_id, opening_inventory, production, donation, closing_inventory}, ...]

    conn = get_db()
    c = conn.cursor()

    try:
        for item in products:
            product_id = item['product_id']
            opening_inventory = item.get('opening_inventory', 0) or 0
            production = item.get('production', 0) or 0
            donation = item.get('donation', 0) or 0
            closing_inventory = item.get('closing_inventory', 0) or 0

            # 모든 값이 0이면 기존 레코드 삭제
            if all(float(v) == 0 for v in [opening_inventory, production, donation, closing_inventory]):
                c.execute('''DELETE FROM irregular_product_records
                            WHERE product_id = ? AND record_date = ?''',
                         (product_id, record_date))
            else:
                # 해당 날짜의 레코드가 있는지 확인
                existing = c.execute('''SELECT id FROM irregular_product_records
                                       WHERE product_id = ? AND record_date = ?''',
                                    (product_id, record_date)).fetchone()

                if existing:
                    # 업데이트
                    c.execute('''UPDATE irregular_product_records
                                SET opening_inventory = ?, production = ?,
                                    donation = ?, closing_inventory = ?
                                WHERE product_id = ? AND record_date = ?''',
                             (opening_inventory, production, donation, closing_inventory,
                              product_id, record_date))
                else:
                    # 새로 추가
                    c.execute('''INSERT INTO irregular_product_records
                                (product_id, opening_inventory, production, donation,
                                 closing_inventory, record_date, note)
                                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                             (product_id, opening_inventory, production, donation,
                              closing_inventory, record_date, ''))

        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# ==================== 판매 관리 API ====================

# 특정 날짜의 모든 제품 판매량 조회 (정기 + 비정기, 생산 - 재고로 자동 계산)
@app.route('/api/sales/grid', methods=['GET'])
def get_sales_grid():
    sales_date = request.args.get('date')

    # 전날 날짜 계산
    from datetime import datetime, timedelta
    current_date = datetime.strptime(sales_date, '%Y-%m-%d')
    prev_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')

    conn = get_db()

    # 1. 정기 제품 조회 (생산량, 재고 기반)
    regular_query = '''SELECT p.id, p.name, p.unit, p.category, p.price, p.cost, p.stock_type,
                      COALESCE(prev_inv.quantity, 0) as opening_inventory,
                      COALESCE(prod.quantity, 0) as production,
                      COALESCE(curr_inv.quantity, 0) as closing_inventory,
                      0 as donation
               FROM products p
               LEFT JOIN inventory_records prev_inv
                   ON p.id = prev_inv.product_id AND prev_inv.inventory_date = ?
               LEFT JOIN production_records prod
                   ON p.id = prod.product_id AND prod.production_date = ?
               LEFT JOIN inventory_records curr_inv
                   ON p.id = curr_inv.product_id AND curr_inv.inventory_date = ?
               WHERE p.category != '비정기 제품'
               ORDER BY p.display_order, p.name'''

    # 2. 비정기 제품 조회 (irregular_product_records 기반)
    irregular_query = '''SELECT p.id, p.name, p.unit, p.category, p.price, p.cost, p.stock_type,
                      COALESCE(ir.opening_inventory, 0) as opening_inventory,
                      COALESCE(ir.production, 0) as production,
                      COALESCE(ir.closing_inventory, 0) as closing_inventory,
                      COALESCE(ir.donation, 0) as donation
               FROM products p
               LEFT JOIN irregular_product_records ir
                   ON p.id = ir.product_id AND ir.record_date = ?
               WHERE p.category = '비정기 제품'
               ORDER BY p.display_order, p.name'''

    regular_results = conn.execute(regular_query, [prev_date, sales_date, sales_date]).fetchall()
    irregular_results = conn.execute(irregular_query, [sales_date]).fetchall()
    conn.close()

    # 판매량 계산
    sales_data = []

    # 정기 제품: 판매량 = 기초재고 + 생산 - 기말재고
    for row in regular_results:
        data = dict(row)
        data['sales'] = data['opening_inventory'] + data['production'] - data['closing_inventory']
        sales_data.append(data)

    # 비정기 제품: 판매량 = 기초재고 + 생산 - 기부 - 기말재고
    for row in irregular_results:
        data = dict(row)
        data['sales'] = data['opening_inventory'] + data['production'] - data['donation'] - data['closing_inventory']
        sales_data.append(data)

    return jsonify(sales_data)

# 판매량은 생산 - 재고로 자동 계산되므로 별도 저장 API 불필요

# ==================== 기부 관리 API ====================

# 특정 날짜의 기부 데이터 조회
@app.route('/api/donation/grid', methods=['GET'])
def get_donation_grid():
    donation_date = request.args.get('date')

    conn = get_db()

    # 1. 정기 제품 중 재고 방식이 '일반'인 제품의 재고량 (기부량)
    regular_query = '''SELECT p.id, p.name, p.unit, p.category, p.price, p.cost, p.stock_type,
                      COALESCE(ir.quantity, 0) as donation_quantity,
                      '정기' as product_type
               FROM products p
               LEFT JOIN inventory_records ir
                   ON p.id = ir.product_id AND ir.inventory_date = ?
               WHERE p.category != '비정기 제품' AND p.stock_type = '일반'
               ORDER BY p.display_order, p.name'''

    # 2. 비정기 제품 중 기부량이 1 이상인 제품
    irregular_query = '''SELECT p.id, p.name, p.unit, p.category, p.price, p.cost, p.stock_type,
                      COALESCE(ir.donation, 0) as donation_quantity,
                      '비정기' as product_type
               FROM products p
               LEFT JOIN irregular_product_records ir
                   ON p.id = ir.product_id AND ir.record_date = ?
               WHERE p.category = '비정기 제품' AND COALESCE(ir.donation, 0) >= 1
               ORDER BY p.display_order, p.name'''

    regular_results = conn.execute(regular_query, [donation_date]).fetchall()
    irregular_results = conn.execute(irregular_query, [donation_date]).fetchall()
    conn.close()

    # 결과 합치기
    donation_data = []
    for row in regular_results:
        donation_data.append(dict(row))
    for row in irregular_results:
        donation_data.append(dict(row))

    return jsonify(donation_data)

# ==================== 자재 입고 관리 API ====================

# 자재 입고 이력 조회
@app.route('/api/material-receipts', methods=['GET'])
def get_material_receipts():
    material_id = request.args.get('material_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    conn = get_db()
    query = '''SELECT mr.*, m.name as material_name, m.unit, m.type
               FROM material_receipts mr
               JOIN materials m ON mr.material_id = m.id
               WHERE 1=1'''
    params = []

    if material_id:
        query += ' AND mr.material_id = ?'
        params.append(material_id)

    if start_date:
        query += ' AND mr.receipt_date >= ?'
        params.append(start_date)

    if end_date:
        query += ' AND mr.receipt_date <= ?'
        params.append(end_date)

    query += ' ORDER BY mr.receipt_date DESC, mr.created_at DESC'

    receipts = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in receipts])

# 자재 입고 등록
@app.route('/api/material-receipts', methods=['POST'])
def add_material_receipt():
    data = request.json
    material_id = data['material_id']
    receipt_date = data['receipt_date']
    quantity = data['quantity']
    unit_price = data['unit_price']
    supplier = data.get('supplier', '')
    note = data.get('note', '')

    conn = get_db()
    c = conn.cursor()

    try:
        c.execute('''INSERT INTO material_receipts
                    (material_id, receipt_date, quantity, unit_price, supplier, note)
                    VALUES (?, ?, ?, ?, ?, ?)''',
                 (material_id, receipt_date, quantity, unit_price, supplier, note))

        # 자재의 평균 단가 업데이트
        update_material_average_price(c, material_id)

        conn.commit()
        receipt_id = c.lastrowid
        conn.close()
        return jsonify({'success': True, 'id': receipt_id})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# 자재 입고 수정
@app.route('/api/material-receipts/<int:receipt_id>', methods=['PUT'])
def update_material_receipt(receipt_id):
    data = request.json
    quantity = data['quantity']
    unit_price = data['unit_price']
    supplier = data.get('supplier', '')
    note = data.get('note', '')

    conn = get_db()
    c = conn.cursor()

    try:
        # 해당 입고 기록의 자재 ID 조회
        receipt = c.execute('SELECT material_id FROM material_receipts WHERE id = ?', [receipt_id]).fetchone()
        if not receipt:
            conn.close()
            return jsonify({'success': False, 'error': '입고 기록을 찾을 수 없습니다'}), 404

        material_id = receipt['material_id']

        c.execute('''UPDATE material_receipts
                    SET quantity = ?, unit_price = ?, supplier = ?, note = ?
                    WHERE id = ?''',
                 (quantity, unit_price, supplier, note, receipt_id))

        # 자재의 평균 단가 업데이트
        update_material_average_price(c, material_id)

        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# 자재 입고 삭제
@app.route('/api/material-receipts/<int:receipt_id>', methods=['DELETE'])
def delete_material_receipt(receipt_id):
    conn = get_db()
    c = conn.cursor()

    try:
        # 해당 입고 기록의 자재 ID 조회
        receipt = c.execute('SELECT material_id FROM material_receipts WHERE id = ?', [receipt_id]).fetchone()
        if not receipt:
            conn.close()
            return jsonify({'success': False, 'error': '입고 기록을 찾을 수 없습니다'}), 404

        material_id = receipt['material_id']

        c.execute('DELETE FROM material_receipts WHERE id = ?', [receipt_id])

        # 자재의 평균 단가 업데이트
        update_material_average_price(c, material_id)

        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 400

# 자재별 평균 단가 계산 및 업데이트 (가중평균법)
def update_material_average_price(cursor, material_id):
    # 모든 입고 이력의 가중평균 계산
    result = cursor.execute('''
        SELECT SUM(quantity * unit_price) / SUM(quantity) as avg_price
        FROM material_receipts
        WHERE material_id = ?
    ''', [material_id]).fetchone()

    avg_price = result['avg_price'] if result['avg_price'] else 0

    # materials 테이블의 unit_price 업데이트
    cursor.execute('UPDATE materials SET unit_price = ? WHERE id = ?', (avg_price, material_id))

# 자재별 현재 평균 단가 조회
@app.route('/api/materials/<int:material_id>/average-price', methods=['GET'])
def get_material_average_price(material_id):
    conn = get_db()

    # 입고 이력 기반 가중평균 계산
    result = conn.execute('''
        SELECT
            SUM(quantity * unit_price) / SUM(quantity) as avg_price,
            SUM(quantity) as total_quantity,
            COUNT(*) as receipt_count
        FROM material_receipts
        WHERE material_id = ?
    ''', [material_id]).fetchone()

    conn.close()

    if result and result['receipt_count'] > 0:
        return jsonify({
            'success': True,
            'avg_price': float(result['avg_price']) if result['avg_price'] else 0,
            'total_quantity': float(result['total_quantity']) if result['total_quantity'] else 0,
            'receipt_count': result['receipt_count']
        })
    else:
        return jsonify({
            'success': True,
            'avg_price': 0,
            'total_quantity': 0,
            'receipt_count': 0
        })

@app.route('/api/dashboard/data')
def get_dashboard_data():
    days = request.args.get('days', 30, type=int)

    conn = get_db()

    # 기준 날짜 계산
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)

    # 1. 일별 판매 추이 데이터 - 정기 제품
    regular_daily_query = '''
        SELECT
            pr.production_date as date,
            SUM(pr.quantity * p.price) as sales
        FROM production_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.production_date <= ?
            AND p.category != '비정기 제품'
        GROUP BY pr.production_date
        ORDER BY pr.production_date
    '''

    # 일별 판매 추이 데이터 - 비정기 제품
    irregular_daily_query = '''
        SELECT
            ir.record_date as date,
            SUM((ir.opening_inventory + ir.production - ir.donation - ir.closing_inventory) * p.price) as sales
        FROM irregular_product_records ir
        JOIN products p ON ir.product_id = p.id
        WHERE ir.record_date >= ? AND ir.record_date <= ?
            AND p.category = '비정기 제품'
        GROUP BY ir.record_date
        ORDER BY ir.record_date
    '''

    regular_daily = conn.execute(regular_daily_query, [start_date, end_date]).fetchall()
    irregular_daily = conn.execute(irregular_daily_query, [start_date, end_date]).fetchall()

    # 날짜별로 데이터 병합
    daily_sales_dict = {}
    for row in regular_daily:
        date_str = row['date']
        daily_sales_dict[date_str] = {
            'date': date_str,
            'regular_sales': float(row['sales']) if row['sales'] else 0,
            'irregular_sales': 0
        }

    for row in irregular_daily:
        date_str = row['date']
        if date_str in daily_sales_dict:
            daily_sales_dict[date_str]['irregular_sales'] = float(row['sales']) if row['sales'] else 0
        else:
            daily_sales_dict[date_str] = {
                'date': date_str,
                'regular_sales': 0,
                'irregular_sales': float(row['sales']) if row['sales'] else 0
            }

    daily_sales = sorted(daily_sales_dict.values(), key=lambda x: x['date'])

    # 2. 카테고리별 판매 분포
    category_sales_query = '''
        SELECT
            COALESCE(p.category, '기타') as category,
            SUM(pr.quantity * p.price) as total_sales
        FROM production_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.production_date <= ?
        GROUP BY p.category
        ORDER BY total_sales DESC
    '''

    category_sales = conn.execute(category_sales_query, [start_date, end_date]).fetchall()

    # 3. 상위 제품 판매 (TOP 5)
    top_products_query = '''
        SELECT
            p.name,
            SUM(pr.quantity) as total_quantity,
            SUM(pr.quantity * p.price) as total_sales
        FROM production_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.production_date <= ?
        GROUP BY p.id, p.name
        ORDER BY total_sales DESC
        LIMIT 5
    '''

    top_products = conn.execute(top_products_query, [start_date, end_date]).fetchall()

    # 4. 하위 제품 판매 (WORST 5)
    worst_products_query = '''
        SELECT
            p.name,
            SUM(pr.quantity) as total_quantity,
            SUM(pr.quantity * p.price) as total_sales
        FROM production_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.production_date <= ?
        GROUP BY p.id, p.name
        ORDER BY total_sales ASC
        LIMIT 5
    '''

    worst_products = conn.execute(worst_products_query, [start_date, end_date]).fetchall()

    # 5. 전체 통계
    stats_query = '''
        SELECT
            SUM(pr.quantity * p.price) as total_sales,
            SUM(pr.quantity * p.cost) as total_cost,
            SUM(pr.quantity * (p.price - p.cost)) as total_margin
        FROM production_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.production_date <= ?
    '''

    stats = conn.execute(stats_query, [start_date, end_date]).fetchone()

    # 6. 기부 금액 계산
    # 정기 제품 중 재고 방식이 '일반'인 제품의 재고량 * 가격
    regular_donation_query = '''
        SELECT SUM(ir.quantity * p.price) as total_donation
        FROM inventory_records ir
        JOIN products p ON ir.product_id = p.id
        WHERE ir.inventory_date >= ? AND ir.inventory_date <= ?
            AND p.category != '비정기 제품' AND p.stock_type = '일반'
    '''

    # 비정기 제품의 기부량 * 가격
    irregular_donation_query = '''
        SELECT SUM(ir.donation * p.price) as total_donation
        FROM irregular_product_records ir
        JOIN products p ON ir.product_id = p.id
        WHERE ir.record_date >= ? AND ir.record_date <= ?
            AND p.category = '비정기 제품'
    '''

    regular_donation = conn.execute(regular_donation_query, [start_date, end_date]).fetchone()
    irregular_donation = conn.execute(irregular_donation_query, [start_date, end_date]).fetchone()

    total_donation = (float(regular_donation['total_donation']) if regular_donation['total_donation'] else 0) + \
                     (float(irregular_donation['total_donation']) if irregular_donation['total_donation'] else 0)

    conn.close()

    # 결과 포맷팅
    dashboard_data = {
        'daily_sales': [
            {
                'date': row['date'],
                'regular_sales': row['regular_sales'],
                'irregular_sales': row['irregular_sales'],
                'total_sales': row['regular_sales'] + row['irregular_sales']
            }
            for row in daily_sales
        ],
        'category_sales': [
            {
                'category': row['category'],
                'total_sales': float(row['total_sales']) if row['total_sales'] else 0
            }
            for row in category_sales
        ],
        'top_products': [
            {
                'name': row['name'],
                'total_quantity': float(row['total_quantity']) if row['total_quantity'] else 0,
                'total_sales': float(row['total_sales']) if row['total_sales'] else 0
            }
            for row in top_products
        ],
        'worst_products': [
            {
                'name': row['name'],
                'total_quantity': float(row['total_quantity']) if row['total_quantity'] else 0,
                'total_sales': float(row['total_sales']) if row['total_sales'] else 0
            }
            for row in worst_products
        ],
        'stats': {
            'total_sales': float(stats['total_sales']) if stats['total_sales'] else 0,
            'total_cost': float(stats['total_cost']) if stats['total_cost'] else 0,
            'total_margin': float(stats['total_margin']) if stats['total_margin'] else 0,
            'total_donation': total_donation,
            'avg_margin_rate': ((float(stats['total_margin']) / float(stats['total_sales']) * 100)
                               if stats['total_sales'] and stats['total_sales'] > 0 else 0)
        }
    }

    return jsonify(dashboard_data)

# ===== Ecount API 엔드포인트 =====

# Ecount 설정 저장
@app.route('/api/ecount/settings', methods=['POST'])
def save_ecount_settings():
    data = request.json
    conn = get_db()

    # 기존 설정 비활성화
    conn.execute('UPDATE ecount_settings SET is_active = 0')

    # 새 설정 추가
    conn.execute('''INSERT INTO ecount_settings
                    (com_code, user_id, zone, api_cert_key, lan_type, is_active)
                    VALUES (?, ?, ?, ?, ?, 1)''',
                 (data['com_code'], data['user_id'], data['zone'],
                  data['api_cert_key'], data.get('lan_type', 'ko-KR')))
    conn.commit()
    conn.close()

    return jsonify({'message': 'Ecount 설정이 저장되었습니다.'})

# Ecount 설정 조회
@app.route('/api/ecount/settings', methods=['GET'])
def get_ecount_settings_api():
    settings = get_ecount_settings()
    if settings:
        # API 키는 마스킹 처리
        settings['api_cert_key'] = settings['api_cert_key'][:4] + '****' + settings['api_cert_key'][-4:]
        return jsonify(settings)
    return jsonify(None)

# Ecount 연결 테스트
@app.route('/api/ecount/test', methods=['POST'])
def test_ecount_connection():
    settings = get_ecount_settings()
    if not settings:
        return jsonify({'success': False, 'error': 'Ecount 설정이 없습니다.'}), 400

    result = ecount_login(settings)
    return jsonify(result)

# 생산 실적을 Ecount 판매로 동기화
@app.route('/api/ecount/sync/production/<int:production_id>', methods=['POST'])
def sync_production_to_ecount(production_id):
    result = sync_production_to_ecount_sale(production_id)
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

# 자재 입고를 Ecount 매입으로 동기화
@app.route('/api/ecount/sync/receipt/<int:receipt_id>', methods=['POST'])
def sync_receipt_to_ecount(receipt_id):
    result = sync_receipt_to_ecount_purchase(receipt_id)
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

# 여러 생산 실적을 한번에 동기화
@app.route('/api/ecount/sync/production/batch', methods=['POST'])
def sync_production_batch():
    data = request.json
    production_ids = data.get('production_ids', [])

    results = []
    for prod_id in production_ids:
        result = sync_production_to_ecount_sale(prod_id)
        results.append({
            'production_id': prod_id,
            'success': result['success'],
            'error': result.get('error')
        })

    success_count = sum(1 for r in results if r['success'])
    return jsonify({
        'total': len(results),
        'success': success_count,
        'failed': len(results) - success_count,
        'results': results
    })

# 여러 입고 기록을 한번에 동기화
@app.route('/api/ecount/sync/receipt/batch', methods=['POST'])
def sync_receipt_batch():
    data = request.json
    receipt_ids = data.get('receipt_ids', [])

    results = []
    for receipt_id in receipt_ids:
        result = sync_receipt_to_ecount_purchase(receipt_id)
        results.append({
            'receipt_id': receipt_id,
            'success': result['success'],
            'error': result.get('error')
        })

    success_count = sum(1 for r in results if r['success'])
    return jsonify({
        'total': len(results),
        'success': success_count,
        'failed': len(results) - success_count,
        'results': results
    })

# 동기화 로그 조회
@app.route('/api/ecount/sync-logs', methods=['GET'])
def get_ecount_sync_logs():
    limit = request.args.get('limit', 100, type=int)
    sync_type = request.args.get('type')

    conn = get_db()

    if sync_type:
        logs = conn.execute('''SELECT * FROM ecount_sync_logs
                              WHERE sync_type = ?
                              ORDER BY created_at DESC LIMIT ?''',
                           (sync_type, limit)).fetchall()
    else:
        logs = conn.execute('''SELECT * FROM ecount_sync_logs
                              ORDER BY created_at DESC LIMIT ?''',
                           (limit,)).fetchall()

    conn.close()
    return jsonify([dict(log) for log in logs])

# 동기화 통계
@app.route('/api/ecount/sync-stats', methods=['GET'])
def get_ecount_sync_stats():
    conn = get_db()

    stats = {
        'total': conn.execute('SELECT COUNT(*) as count FROM ecount_sync_logs').fetchone()['count'],
        'success': conn.execute("SELECT COUNT(*) as count FROM ecount_sync_logs WHERE status = 'success'").fetchone()['count'],
        'failed': conn.execute("SELECT COUNT(*) as count FROM ecount_sync_logs WHERE status = 'failed'").fetchone()['count'],
        'by_type': {}
    }

    type_stats = conn.execute('''SELECT sync_type, status, COUNT(*) as count
                                 FROM ecount_sync_logs
                                 GROUP BY sync_type, status''').fetchall()

    for row in type_stats:
        sync_type = row['sync_type']
        if sync_type not in stats['by_type']:
            stats['by_type'][sync_type] = {'success': 0, 'failed': 0}
        stats['by_type'][sync_type][row['status']] = row['count']

    conn.close()
    return jsonify(stats)

# CSV 파일에서 이카운트 제품 코드 매칭
@app.route('/api/ecount/match-products', methods=['POST'])
def match_ecount_products():
    """CSV 데이터로 제품명 매칭하여 이카운트 코드 제안"""
    try:
        data = request.json
        csv_data = data.get('csv_data', [])  # [{"code": "00001A", "name": "크루아상(발효)"}, ...]

        if not csv_data:
            return jsonify({'success': False, 'error': 'CSV 데이터가 없습니다.'}), 400

        conn = get_db()
        products = conn.execute('SELECT id, name, ecount_code FROM products').fetchall()
        # 원자재 유형만 매칭 (이카운트에 등록된 자재 유형)
        materials = conn.execute("SELECT id, name, ecount_code FROM materials WHERE type = '원자재'").fetchall()
        conn.close()

        # 제품 매칭
        product_matches = []
        for product in products:
            product_dict = dict(product)
            # CSV에서 제품명이 일치하는 항목 찾기
            matched_csv = next((item for item in csv_data if item['name'] == product_dict['name']), None)

            product_matches.append({
                'id': product_dict['id'],
                'name': product_dict['name'],
                'current_code': product_dict['ecount_code'],
                'suggested_code': matched_csv['code'] if matched_csv else None,
                'matched': matched_csv is not None,
                'type': 'product'
            })

        # 자재 매칭
        material_matches = []
        for material in materials:
            material_dict = dict(material)
            # CSV에서 자재명이 일치하는 항목 찾기
            matched_csv = next((item for item in csv_data if item['name'] == material_dict['name']), None)

            material_matches.append({
                'id': material_dict['id'],
                'name': material_dict['name'],
                'current_code': material_dict['ecount_code'],
                'suggested_code': matched_csv['code'] if matched_csv else None,
                'matched': matched_csv is not None,
                'type': 'material'
            })

        return jsonify({
            'success': True,
            'product_matches': product_matches,
            'material_matches': material_matches,
            'total_products': len(product_matches),
            'matched_products': sum(1 for p in product_matches if p['matched']),
            'total_materials': len(material_matches),
            'matched_materials': sum(1 for m in material_matches if m['matched'])
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

# 매칭된 이카운트 코드 일괄 적용
@app.route('/api/ecount/apply-matches', methods=['POST'])
def apply_ecount_matches():
    """매칭 결과를 데이터베이스에 일괄 적용"""
    try:
        data = request.json
        matches = data.get('matches', [])  # [{"id": 1, "type": "product", "ecount_code": "00001A"}, ...]

        if not matches:
            return jsonify({'success': False, 'error': '적용할 매칭 데이터가 없습니다.'}), 400

        conn = get_db()
        c = conn.cursor()

        updated_products = 0
        updated_materials = 0

        for match in matches:
            item_id = match['id']
            item_type = match['type']
            ecount_code = match['ecount_code']

            if item_type == 'product':
                c.execute('UPDATE products SET ecount_code = ? WHERE id = ?', (ecount_code, item_id))
                updated_products += 1
            elif item_type == 'material':
                c.execute('UPDATE materials SET ecount_code = ? WHERE id = ?', (ecount_code, item_id))
                updated_materials += 1

        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'updated_products': updated_products,
            'updated_materials': updated_materials,
            'message': f'제품 {updated_products}개, 자재 {updated_materials}개의 이카운트 코드가 업데이트되었습니다.'
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

init_db()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
