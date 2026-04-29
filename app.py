from flask import Flask, render_template, jsonify, request
import sqlite3
import requests
from datetime import datetime, timedelta, date, timezone
import os

KST = timezone(timedelta(hours=9))

app = Flask(__name__)

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rankings.db')
NAVER_CLIENT_ID = os.environ.get('NAVER_CLIENT_ID', 'Qsttx7Y7J7UrVDvv6uER')
NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET', 's7myjV0Muj')

DEFAULT_PRODUCTS = [
    ('키위', 'https://smartstore.naver.com/sweetiefood/products/13360154869', '13360154869'),
]
# 상품 product_id → 기본 키워드
DEFAULT_KW = {
    '13360154869': ['키위'],
}


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        url TEXT NOT NULL,
        product_id TEXT NOT NULL UNIQUE,
        image_url TEXT DEFAULT "",
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # 상품별 키워드 (기존 global keywords 대체)
    c.execute('''CREATE TABLE IF NOT EXISTS product_keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        keyword TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(product_id, keyword)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS rankings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        keyword_id INTEGER,
        rank INTEGER,
        total_results INTEGER,
        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS datalab_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT NOT NULL UNIQUE,
        ratio REAL,
        peak_ratio REAL,
        peak_period TEXT,
        trend TEXT,
        cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    # safe migration
    try:
        c.execute('ALTER TABLE products ADD COLUMN image_url TEXT DEFAULT ""')
    except Exception:
        pass

    for name, url, product_id in DEFAULT_PRODUCTS:
        c.execute('INSERT OR IGNORE INTO products (name, url, product_id) VALUES (?, ?, ?)',
                  (name, url, product_id))

    conn.commit()

    # 기본 키워드: 잘못된 매핑 삭제 후 올바른 상품에만 삽입
    for name, url, product_id in DEFAULT_PRODUCTS:
        row = conn.execute('SELECT id FROM products WHERE product_id = ?', (product_id,)).fetchone()
        if not row:
            continue
        pid = row['id']
        for kw in DEFAULT_KW.get(product_id, []):
            # 이 키워드가 다른 상품에 매핑되어 있으면 삭제
            conn.execute('DELETE FROM product_keywords WHERE keyword=? AND product_id!=?', (kw, pid))
            # 올바른 상품에 삽입
            conn.execute('INSERT OR IGNORE INTO product_keywords (product_id, keyword) VALUES (?, ?)', (pid, kw))

    conn.commit()
    conn.close()


def naver_headers():
    return {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
    }


def extract_product_id(url):
    return url.rstrip('/').split('/')[-1]


def search_and_find(keyword, product_id, max_rank=1000):
    """페이지 단위로 검색하다가 상품 발견 즉시 반환 (속도 최적화)"""
    display = 100
    ss_rank = 0
    total = 0
    found_img = ''
    for page in range(max_rank // display):
        start = page * display + 1
        try:
            r = requests.get(
                'https://openapi.naver.com/v1/search/shop.json',
                headers=naver_headers(),
                params={'query': keyword, 'display': display, 'start': start, 'sort': 'sim'},
                timeout=15
            )
            if r.status_code != 200:
                break
            data = r.json()
            if page == 0:
                total = data.get('total', 0)
            items = data.get('items', [])
            for item in items:
                if 'smartstore.naver.com' in item.get('link', ''):
                    ss_rank += 1
                    if product_id in item.get('link', ''):
                        return ss_rank, item.get('image', ''), total
            if len(items) < display:
                break
        except Exception:
            break
    return None, found_img, total


# 하위 호환용 래퍼
def search_shopping(keyword, max_rank=1000):
    display = 100
    all_items, total = [], 0
    for page in range(max_rank // display):
        start = page * display + 1
        try:
            r = requests.get(
                'https://openapi.naver.com/v1/search/shop.json',
                headers=naver_headers(),
                params={'query': keyword, 'display': display, 'start': start, 'sort': 'sim'},
                timeout=15
            )
            if r.status_code != 200:
                break
            data = r.json()
            if page == 0:
                total = data.get('total', 0)
            items = data.get('items', [])
            all_items.extend(items)
            if len(items) < display:
                break
        except Exception:
            break
    return all_items, total


def find_rank(items, product_id):
    ss_items = [it for it in items if 'smartstore.naver.com' in it.get('link', '')]
    for i, item in enumerate(ss_items):
        if product_id in item.get('link', ''):
            return i + 1, item.get('image', '')
    return None, None


def fetch_product_image(product_name, product_id):
    try:
        r = requests.get(
            'https://openapi.naver.com/v1/search/shop.json',
            headers=naver_headers(),
            params={'query': f'스위티푸드 {product_name}', 'display': 20, 'sort': 'sim'},
            timeout=10
        )
        if r.status_code == 200:
            for item in r.json().get('items', []):
                if product_id in item.get('link', ''):
                    img = item.get('image', '')
                    if img:
                        return img
    except Exception:
        pass
    return ''


def get_datalab(keyword):
    conn = get_db()
    cached = conn.execute(
        "SELECT * FROM datalab_cache WHERE keyword=? AND cached_at >= datetime('now','-24 hours')",
        (keyword,)
    ).fetchone()
    if cached:
        conn.close()
        return dict(cached)

    today = date.today()
    end_dt = today - timedelta(days=1)
    start_dt = end_dt - timedelta(days=365)
    try:
        r = requests.post(
            'https://openapi.naver.com/v1/datalab/search',
            headers={**naver_headers(), 'Content-Type': 'application/json'},
            json={
                'startDate': start_dt.strftime('%Y-%m-%d'),
                'endDate': end_dt.strftime('%Y-%m-%d'),
                'timeUnit': 'week',
                'keywordGroups': [{'groupName': keyword, 'keywords': [keyword]}]
            },
            timeout=15
        )
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results:
                pts = results[0].get('data', [])
                if pts:
                    # 마지막 주는 미완성(1~2일치)일 수 있어 완성된 직전 주 사용
                    cur_pt = pts[-2] if len(pts) >= 2 else pts[-1]
                    current = cur_pt.get('ratio', 0)
                    peak = max(d.get('ratio', 0) for d in pts)
                    peak_period = max(pts, key=lambda d: d.get('ratio', 0)).get('period', '')[:7]
                    trend = 'up' if len(pts) >= 3 and pts[-2]['ratio'] > pts[-3]['ratio'] else 'down'
                    conn.execute('''INSERT OR REPLACE INTO datalab_cache
                        (keyword,ratio,peak_ratio,peak_period,trend,cached_at)
                        VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)''',
                        (keyword, round(current, 1), round(peak, 1), peak_period, trend))
                    conn.commit()
                    conn.close()
                    return {'keyword': keyword, 'ratio': round(current, 1),
                            'peak_ratio': round(peak, 1), 'peak_period': peak_period, 'trend': trend}
    except Exception:
        pass
    conn.close()
    return None


def competition_label(total):
    if not total:
        return '-'
    if total >= 500000:
        return '매우 높음'
    if total >= 100000:
        return '높음'
    if total >= 30000:
        return '보통'
    if total >= 5000:
        return '낮음'
    return '매우 낮음'


# ─── Routes ───────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/products', methods=['GET'])
def get_products():
    conn = get_db()
    rows = conn.execute('SELECT * FROM products ORDER BY id').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/products', methods=['POST'])
def add_product():
    data = request.get_json()
    name = data.get('name', '').strip()
    url = data.get('url', '').strip()
    if not name or not url:
        return jsonify({'error': '상품명과 URL을 입력하세요'}), 400
    product_id = extract_product_id(url)
    image_url = fetch_product_image(name, product_id)
    conn = get_db()
    try:
        conn.execute('INSERT INTO products (name, url, product_id, image_url) VALUES (?, ?, ?, ?)',
                     (name, url, product_id, image_url))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': '이미 등록된 상품입니다'}), 400
    conn.close()
    return jsonify({'success': True})


@app.route('/api/products/<int:pid>', methods=['DELETE'])
def delete_product(pid):
    conn = get_db()
    conn.execute('DELETE FROM products WHERE id=?', (pid,))
    conn.execute('DELETE FROM product_keywords WHERE product_id=?', (pid,))
    conn.execute('DELETE FROM rankings WHERE product_id=?', (pid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/products/<int:pid>/keywords', methods=['GET'])
def get_product_keywords(pid):
    conn = get_db()
    rows = conn.execute('SELECT * FROM product_keywords WHERE product_id=? ORDER BY id', (pid,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/products/<int:pid>/keywords', methods=['POST'])
def add_product_keyword(pid):
    data = request.get_json()
    keyword = data.get('keyword', '').strip()
    if not keyword:
        return jsonify({'error': '키워드를 입력하세요'}), 400
    conn = get_db()
    try:
        conn.execute('INSERT INTO product_keywords (product_id, keyword) VALUES (?, ?)', (pid, keyword))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': '이미 등록된 키워드입니다'}), 400
    conn.close()
    return jsonify({'success': True})


@app.route('/api/product_keywords/<int:kid>', methods=['DELETE'])
def delete_product_keyword(kid):
    conn = get_db()
    conn.execute('DELETE FROM product_keywords WHERE id=?', (kid,))
    conn.execute('DELETE FROM rankings WHERE keyword_id=?', (kid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/check_product/<int:pid>', methods=['POST'])
def check_product(pid):
    conn = get_db()
    product = conn.execute('SELECT * FROM products WHERE id=?', (pid,)).fetchone()
    keywords = conn.execute('SELECT * FROM product_keywords WHERE product_id=?', (pid,)).fetchall()
    conn.close()

    if not product:
        return jsonify({'error': '상품을 찾을 수 없습니다'}), 404

    product = dict(product)
    now = datetime.now(KST)
    results = []

    for kw_row in keywords:
        kw = dict(kw_row)
        rank, img, total = search_and_find(kw['keyword'], product['product_id'])

        conn = get_db()
        conn.execute(
            'INSERT INTO rankings (product_id, keyword_id, rank, total_results, checked_at) VALUES (?,?,?,?,?)',
            (pid, kw["id"], rank, total, now.strftime("%Y-%m-%d %H:%M:%S"))
        )
        if img and not product.get('image_url'):
            conn.execute('UPDATE products SET image_url=? WHERE id=?', (img, pid))
            product['image_url'] = img
        conn.commit()
        conn.close()

        results.append({'keyword': kw['keyword'], 'rank': rank, 'total': total})

    return jsonify({'success': True, 'results': results, 'checked_at': now.strftime('%Y-%m-%d %H:%M')})


@app.route('/api/check_all', methods=['POST'])
def check_all():
    conn = get_db()
    products = [dict(r) for r in conn.execute('SELECT * FROM products ORDER BY id').fetchall()]
    conn.close()

    now = datetime.now(KST)
    for p in products:
        conn = get_db()
        keywords = [dict(r) for r in conn.execute(
            'SELECT * FROM product_keywords WHERE product_id=?', (p['id'],)
        ).fetchall()]
        conn.close()

        for kw in keywords:
            rank, img, total = search_and_find(kw['keyword'], p['product_id'])
            conn = get_db()
            conn.execute(
                'INSERT INTO rankings (product_id, keyword_id, rank, total_results, checked_at) VALUES (?,?,?,?,?)',
                (p["id"], kw["id"], rank, total, now.strftime("%Y-%m-%d %H:%M:%S"))
            )
            if img and not p.get('image_url'):
                conn.execute('UPDATE products SET image_url=? WHERE id=?', (img, p['id']))
                p['image_url'] = img
            conn.commit()
            conn.close()

    return jsonify({'success': True, 'checked_at': now.strftime('%Y-%m-%d %H:%M')})


@app.route('/api/full_data')
def full_data():
    conn = get_db()
    products = [dict(r) for r in conn.execute('SELECT * FROM products ORDER BY id').fetchall()]

    now = datetime.now(KST)
    yesterday_str = (now.date() - timedelta(days=1)).isoformat()
    week_ago = (now - timedelta(days=7)).isoformat()

    result = []
    for p in products:
        keywords = [dict(r) for r in conn.execute(
            'SELECT * FROM product_keywords WHERE product_id=? ORDER BY id', (p['id'],)
        ).fetchall()]

        kw_data = []
        for k in keywords:
            latest = conn.execute('''
                SELECT rank, total_results, checked_at FROM rankings
                WHERE product_id=? AND keyword_id=?
                ORDER BY checked_at DESC LIMIT 1
            ''', (p['id'], k['id'])).fetchone()

            y_row = conn.execute('''
                SELECT rank FROM rankings
                WHERE product_id=? AND keyword_id=? AND date(checked_at)=?
                ORDER BY checked_at DESC LIMIT 1
            ''', (p['id'], k['id'], yesterday_str)).fetchone()

            w_row = conn.execute('''
                SELECT ROUND(AVG(rank)) as avg_rank FROM rankings
                WHERE product_id=? AND keyword_id=? AND rank IS NOT NULL AND checked_at>=?
            ''', (p['id'], k['id'], week_ago)).fetchone()

            latest = dict(latest) if latest else {}
            dl = get_datalab(k['keyword'])

            kw_data.append({
                'id': k['id'],
                'keyword': k['keyword'],
                'rank': latest.get('rank'),
                'total_results': latest.get('total_results'),
                'checked_at': latest.get('checked_at'),
                'yesterday_rank': dict(y_row)['rank'] if y_row else None,
                'weekly_avg': int(dict(w_row)['avg_rank']) if w_row and dict(w_row)['avg_rank'] else None,
                'competition': competition_label(latest.get('total_results')),
                'datalab': dl,
            })

        result.append({
            'id': p['id'],
            'name': p['name'],
            'url': p['url'],
            'image_url': p.get('image_url', ''),
            'keywords': kw_data,
        })

    conn.close()
    return jsonify(result)


@app.route('/api/debug_search')
def debug_search():
    keyword = request.args.get('keyword', '수박')
    items, total = search_shopping(keyword)
    result = []
    for i, item in enumerate(items[:100]):
        result.append({
            'rank': i + 1,
            'title': item.get('title', ''),
            'link': item.get('link', ''),
            'mallName': item.get('mallName', ''),
            'productId': item.get('productId', ''),
        })
    return jsonify({'total': total, 'count': len(items), 'items': result})


@app.route('/api/reset_db', methods=['POST'])
def reset_db():
    conn = get_db()
    conn.execute('DELETE FROM product_keywords')
    conn.execute('DELETE FROM rankings')
    conn.execute('DELETE FROM datalab_cache')
    conn.execute('DELETE FROM products')
    conn.commit()
    conn.close()
    init_db()
    return jsonify({'success': True, 'msg': 'DB 초기화 완료'})


@app.route('/api/check_keyword', methods=['POST'])
def check_keyword():
    data = request.get_json()
    keyword = data.get('keyword', '').strip()
    if not keyword:
        return jsonify({'error': '키워드를 입력하세요'}), 400

    conn = get_db()
    products = [dict(r) for r in conn.execute('SELECT * FROM products ORDER BY id').fetchall()]
    conn.close()

    now = datetime.now(KST)
    results = []
    for p in products:
        items, total = search_shopping(keyword)
        rank, img = find_rank(items, p['product_id'])

        conn = get_db()
        # keyword_id=None (임시 키워드 조회, DB 저장 안함)
        if img and not p.get('image_url'):
            conn.execute('UPDATE products SET image_url=? WHERE id=?', (img, p['id']))
            conn.commit()
        conn.close()

        results.append({'product_name': p['name'], 'rank': rank, 'total': total})

    return jsonify({'success': True, 'results': results, 'checked_at': now.strftime('%Y-%m-%d %H:%M')})


@app.route('/api/fetch_images', methods=['POST'])
def fetch_images():
    conn = get_db()
    products = [dict(r) for r in conn.execute(
        'SELECT * FROM products WHERE image_url="" OR image_url IS NULL'
    ).fetchall()]
    conn.close()
    for p in products:
        img = fetch_product_image(p['name'], p['product_id'])
        if img:
            conn = get_db()
            conn.execute('UPDATE products SET image_url=? WHERE id=?', (img, p['id']))
            conn.commit()
            conn.close()
    return jsonify({'success': True})


# gunicorn 포함 모든 실행환경에서 DB 초기화
with app.app_context():
    init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=False)
