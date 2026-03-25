"""
RecipeSnap — AI Invoice Scanner for Restaurants
Flask backend with server-side Claude API + Notion integration
v2.1 — multi-page + image conversion
"""
import os, sys, json, base64, time, hmac, hashlib, secrets, traceback, logging
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory, redirect, session, url_for
import anthropic
import requests

# Force logging to stderr so gunicorn/journalctl captures it
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.environ.get('RECIPESNAP_SECRET', secrets.token_hex(32))

# ── Config ──────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
NOTION_CLIENT_ID = os.environ.get('NOTION_CLIENT_ID', '')
NOTION_CLIENT_SECRET = os.environ.get('NOTION_CLIENT_SECRET', '')
NOTION_REDIRECT_URI = os.environ.get('NOTION_REDIRECT_URI', 'https://recipesnap.com/auth/notion/callback')
MAX_SCANS_FREE = 999999  # unlimited for testing
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB

# ── Simple file-based storage (swap for SQLite/Postgres later) ──
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

def get_user_file(user_id):
    return os.path.join(DATA_DIR, f'{user_id}.json')

def load_user(user_id):
    path = get_user_file(user_id)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {
        'id': user_id,
        'created': datetime.now().isoformat(),
        'scan_count': 0,
        'scan_count_month': 0,
        'scan_month': datetime.now().strftime('%Y-%m'),
        'tier': 'free',  # free | basic | pro
        'notion_token': None,
        'notion_database_id': None,
        'scans': []
    }

def save_user(user_data):
    path = get_user_file(user_data['id'])
    with open(path, 'w') as f:
        json.dump(user_data, f, indent=2)

def get_monthly_count(user_data):
    current_month = datetime.now().strftime('%Y-%m')
    if user_data.get('scan_month') != current_month:
        user_data['scan_count_month'] = 0
        user_data['scan_month'] = current_month
    return user_data['scan_count_month']

def get_scan_limit(tier):
    return {'free': MAX_SCANS_FREE, 'basic': 50, 'pro': 150}.get(tier, MAX_SCANS_FREE)


# ── Auth (simple token-based for MVP) ───────────────────
def generate_token(user_id):
    """Simple HMAC token — replace with JWT for production"""
    msg = f'{user_id}:{int(time.time())}'.encode()
    sig = hmac.new(app.secret_key.encode(), msg, hashlib.sha256).hexdigest()[:16]
    return base64.urlsafe_b64encode(f'{user_id}:{int(time.time())}:{sig}'.encode()).decode()

def verify_token(token):
    try:
        decoded = base64.urlsafe_b64decode(token).decode()
        user_id, ts, sig = decoded.rsplit(':', 2)
        return user_id
    except Exception:
        return None

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token:
            token = request.args.get('token', '')
        user_id = verify_token(token)
        if not user_id:
            return jsonify({'error': 'Unauthorized'}), 401
        request.user_id = user_id
        return f(*args, **kwargs)
    return decorated


# ── OCR Prompt (refined from Red Nun sessions 13-32) ────
INVOICE_OCR_PROMPT = """You are an expert invoice OCR system for restaurants. Extract all data from this invoice image.

CRITICAL RULES — PRICES:
- PRICES MUST BE EXACT. Read each dollar amount character by character from the invoice. Do NOT round, estimate, or calculate prices.
- unit_price: copy the EXACT printed price per unit. Do NOT derive it from total ÷ quantity.
- total_price: copy the EXACT printed extension/line total. Do NOT derive it from quantity × unit_price.
- If unit_price and total_price are both printed, use BOTH as-is even if they don't multiply perfectly (rounding, catch-weight, etc.).
- Watch for decimal alignment — $12.50 is not $125.0 and $1.25 is not $12.5.
- Negative amounts (credits, returns) should be negative numbers.

CRITICAL RULES — QUANTITIES:
- Use SHIPPED/DELIVERED quantity, NOT ordered quantity, if both columns exist.
- For catch-weight items the quantity may be a decimal (e.g. 14.35 lbs).

CRITICAL RULES — STRUCTURE:
- Do NOT include Sales Tax, delivery charges, fuel surcharges, or deposit fees as line items. Those go in their own fields.
- If you see a vendor item code / product code, include it as "item_code".
- Look for ship-to address to determine delivery location.
- PACK SIZE: Many items include a pack/size column (e.g. "6/5 LB", "4/1 GAL", "24 CT", "50 LB", "12/16 OZ").
  Parse this into pack_size (the raw text) AND pack_weight_oz (total weight in ounces as a number).
  Conversion: 1 lb = 16 oz, 1 gal = 128 oz, 1 qt = 32 oz, 1 pt = 16 oz.
  Examples: "6/5 LB" = 6×5×16 = 480 oz. "4/1 GAL" = 4×1×128 = 512 oz. "50 LB" = 50×16 = 800 oz. "24 CT" = 24 (use count as-is for countable items like rolls, each).
  If pack/size is missing or unclear, set pack_weight_oz to null.

ORIENTATION:
- The invoice image may be rotated or photographed in portrait while the document is landscape. If the text appears sideways or upside-down, mentally rotate and read it correctly.
- Always read ALL text regardless of orientation.

VENDOR-SPECIFIC:
- US Foods: columns are ITEM # | DESCRIPTION | PACK/SIZE | ORDERED | SHIPPED | UNIT PRICE | EXTENSION — use SHIPPED not ORDERED.
- Sysco: look for "Ship Qty" vs "Ord Qty" — use Ship Qty. Price column is usually "Price" or "Unit Price", extension is "Amount".
- Southern Glazer's: deposit returns are negative values, include them.
- Performance Foodservice: watch for items split across two lines.
- Restaurant Depot: receipt format — item, qty, price on same line.
- Artignetti Companies: liquor distributor — columns include item code, description, pack/size, ordered, shipped, unit price, and extension. Use shipped qty. Prices are per-case.

Return ONLY valid JSON in this exact format:
{
  "vendor_name": "string",
  "invoice_number": "string",
  "invoice_date": "string (MM/DD/YYYY)",
  "due_date": "string or null",
  "ship_to_address": "string or null",
  "subtotal": number,
  "tax": number or 0,
  "delivery_fee": number or 0,
  "total": number,
  "items": [
    {
      "item_name": "string",
      "item_code": "string or null",
      "pack_size": "string or null",
      "pack_weight_oz": number or null,
      "quantity": number,
      "unit_price": number,
      "total_price": number
    }
  ]
}

Return ONLY the JSON object. No markdown, no explanation, no code fences."""


# ── Routes: Static ──────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/scanner')
def scanner():
    return send_from_directory('static', 'index.html')


# ── Routes: Auth ─────────────────────────────────────────
@app.route('/api/auth/demo', methods=['POST'])
def demo_login():
    """Quick demo login — generates a user ID and token"""
    user_id = f"demo_{secrets.token_hex(8)}"
    user_data = load_user(user_id)
    save_user(user_data)
    token = generate_token(user_id)
    return jsonify({
        'token': token,
        'user_id': user_id,
        'tier': user_data['tier'],
        'scans_remaining': get_scan_limit(user_data['tier']) - get_monthly_count(user_data)
    })


# ── Routes: Scan ─────────────────────────────────────────
@app.route('/api/scan', methods=['POST'])
@require_auth
def scan_invoice():
    """Upload an invoice image, get structured data back"""
    logging.info("=== SCAN v2.1 START ===")
    user_data = load_user(request.user_id)
    monthly = get_monthly_count(user_data)
    limit = get_scan_limit(user_data['tier'])

    if monthly >= limit:
        return jsonify({
            'error': 'Monthly scan limit reached',
            'limit': limit,
            'used': monthly,
            'tier': user_data['tier']
        }), 429

    # Collect all uploaded files (supports multiple)
    files = request.files.getlist('file')
    if not files or all(f.filename == '' for f in files):
        return jsonify({'error': 'No file uploaded'}), 400

    # Build content blocks for Claude — supports PDFs, images, and multi-file
    content_blocks = []
    total_size = 0

    for file in files:
        file_data = file.read()
        total_size += len(file_data)
        logging.info(f"  File: {file.filename}, content_type={file.content_type}, size={len(file_data)}")
        if total_size > MAX_FILE_SIZE:
            return jsonify({'error': 'Total file size too large (max 20MB)'}), 413

        b64 = base64.standard_b64encode(file_data).decode('utf-8')
        media_type = file.content_type or 'image/jpeg'

        if media_type == 'application/pdf' or (file.filename and file.filename.lower().endswith('.pdf')):
            # PDF: convert each page to image for Claude Vision
            # (native document blocks can have compatibility issues)
            import io
            try:
                import fitz  # PyMuPDF
                pdf_doc = fitz.open(stream=file_data, filetype="pdf")
                for page_num in range(len(pdf_doc)):
                    page = pdf_doc[page_num]
                    pix = page.get_pixmap(dpi=200)
                    img_data = pix.tobytes("png")
                    img_b64 = base64.standard_b64encode(img_data).decode('utf-8')
                    content_blocks.append({
                        'type': 'image',
                        'source': {'type': 'base64', 'media_type': 'image/png', 'data': img_b64}
                    })
                pdf_doc.close()
            except ImportError:
                # Fallback: send PDF as document block if PyMuPDF not installed
                content_blocks.append({
                    'type': 'document',
                    'source': {'type': 'base64', 'media_type': 'application/pdf', 'data': b64}
                })
        else:
            # Image: enhance + normalize for best OCR accuracy
            from PIL import Image, ImageEnhance, ImageFilter, ExifTags
            import io
            try:
                img = Image.open(io.BytesIO(file_data))

                # Auto-rotate from EXIF (iPhone photos embed orientation)
                try:
                    for orientation in ExifTags.TAGS.keys():
                        if ExifTags.TAGS[orientation] == 'Orientation':
                            break
                    exif = img._getexif()
                    if exif and orientation in exif:
                        if exif[orientation] == 3: img = img.rotate(180, expand=True)
                        elif exif[orientation] == 6: img = img.rotate(270, expand=True)
                        elif exif[orientation] == 8: img = img.rotate(90, expand=True)
                except Exception:
                    pass

                # Convert to RGB if needed
                if img.mode in ('RGBA', 'P', 'LA'):
                    img = img.convert('RGB')

                # Resize if too large (iPhone photos can be 4032x3024 = 12MP)
                # Cap at 2400px on longest side — plenty for OCR, keeps under 5MB
                max_dim = 2400
                if max(img.size) > max_dim:
                    ratio = max_dim / max(img.size)
                    new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
                    img = img.resize(new_size, Image.LANCZOS)
                    logging.info(f"    Resized to {new_size[0]}x{new_size[1]}")

                # Sharpen slightly for better text recognition
                img = img.filter(ImageFilter.SHARPEN)

                # Boost contrast slightly
                img = ImageEnhance.Contrast(img).enhance(1.2)

                # Save as JPEG, compress to stay under 4.5MB (API limit is 5MB)
                quality = 88
                while quality >= 50:
                    buf = io.BytesIO()
                    img.save(buf, format='JPEG', quality=quality)
                    if buf.tell() < 4_500_000:
                        break
                    quality -= 10
                file_data = buf.getvalue()
                b64 = base64.standard_b64encode(file_data).decode('utf-8')
                media_type = 'image/jpeg'
                logging.info(f"    Enhanced image: {img.size[0]}x{img.size[1]}, {len(file_data)//1024}KB, q={quality}")
            except Exception as e:
                logging.warning(f"    Image enhance failed: {e}")
                # Fallback: just ensure valid media type
                SUPPORTED_TYPES = {'image/jpeg', 'image/png', 'image/gif', 'image/webp'}
                if media_type not in SUPPORTED_TYPES:
                    media_type = 'image/jpeg'

            content_blocks.append({
                'type': 'image',
                'source': {'type': 'base64', 'media_type': media_type, 'data': b64}
            })

    # Add the prompt as the last content block
    page_hint = ""
    if len(content_blocks) > 1:
        page_hint = f"\n\nIMPORTANT: This invoice has {len(content_blocks)} pages/images. Extract ALL items from ALL pages into a single combined items list. Do not miss items on later pages."

    content_blocks.append({
        'type': 'text',
        'text': INVOICE_OCR_PROMPT + page_hint
    })

    # Call Claude Vision
    logging.info(f"  Sending {len(content_blocks)} content blocks to Claude API")
    for i, block in enumerate(content_blocks):
        if block['type'] == 'image':
            logging.info(f"    Block {i}: image, media_type={block['source']['media_type']}, data_len={len(block['source']['data'])}")
        elif block['type'] == 'document':
            logging.info(f"    Block {i}: document, media_type={block['source']['media_type']}")
        else:
            logging.info(f"    Block {i}: {block['type']}, text_len={len(block.get('text',''))}")

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=16384,
            messages=[{
                'role': 'user',
                'content': content_blocks
            }]
        )

        result_text = response.content[0].text.strip()
        logging.info(f"  Claude response length: {len(result_text)}")
        # Handle markdown fences
        if result_text.startswith('```'):
            result_text = result_text.replace('```json\n', '').replace('```json', '').replace('```\n', '').replace('```', '')

        invoice_data = json.loads(result_text)

    except json.JSONDecodeError as e:
        logging.error(f"  JSON parse error: {e}\n  Raw text: {result_text[:500]}")
        return jsonify({'error': f'Failed to parse OCR result: {str(e)}', 'raw': result_text}), 500
    except anthropic.APIError as e:
        logging.error(f"  Anthropic API error: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        return jsonify({'error': f'Claude API error: {str(e)}'}), 502
    except Exception as e:
        logging.error(f"  Unexpected error: {type(e).__name__}: {e}")
        logging.error(traceback.format_exc())
        return jsonify({'error': f'Scan error: {str(e)}'}), 500

    # Math validation
    errors = []
    for item in invoice_data.get('items', []):
        expected = round(item['quantity'] * item['unit_price'], 2)
        diff = abs(expected - item['total_price'])
        item['_math_ok'] = diff <= 0.10  # 10 cent tolerance for catch-weight/rounding
        if not item['_math_ok']:
            errors.append(f"{item['item_name']}: expected ${expected:.2f}, got ${item['total_price']:.2f}")

    items_sum = round(sum(i['total_price'] for i in invoice_data.get('items', [])), 2)
    tax = invoice_data.get('tax', 0) or 0
    delivery = invoice_data.get('delivery_fee', 0) or 0
    stated_total = invoice_data.get('total', 0) or 0
    gap = round(stated_total - (items_sum + tax + delivery), 2)

    invoice_data['_validation'] = {
        'items_sum': items_sum,
        'gap': gap,
        'gap_ok': abs(gap) <= 1.00,
        'line_errors': errors
    }

    # Track usage
    user_data['scan_count'] += 1
    user_data['scan_count_month'] = monthly + 1
    user_data['scans'].append({
        'timestamp': datetime.now().isoformat(),
        'vendor': invoice_data.get('vendor_name', 'Unknown'),
        'total': invoice_data.get('total', 0),
        'items_count': len(invoice_data.get('items', []))
    })
    save_user(user_data)

    return jsonify({
        'invoice': invoice_data,
        'usage': {
            'scans_used': monthly + 1,
            'scans_limit': limit,
            'tier': user_data['tier']
        }
    })


# ── Routes: Notion Integration ───────────────────────────
@app.route('/api/notion/connect')
@require_auth
def notion_connect():
    """Start Notion OAuth flow"""
    if not NOTION_CLIENT_ID:
        return jsonify({'error': 'Notion integration not configured'}), 501
    auth_url = (
        f"https://api.notion.com/v1/oauth/authorize"
        f"?client_id={NOTION_CLIENT_ID}"
        f"&response_type=code"
        f"&owner=user"
        f"&redirect_uri={NOTION_REDIRECT_URI}"
        f"&state={request.user_id}"
    )
    return jsonify({'auth_url': auth_url})

@app.route('/api/notion/save-token', methods=['POST'])
@require_auth
def notion_save_token():
    """Save a personal Notion integration token (no OAuth needed)"""
    data = request.json
    notion_token = (data.get('token') or '').strip()
    if not notion_token:
        return jsonify({'error': 'Missing token'}), 400

    # Validate the token by calling Notion API
    resp = requests.get('https://api.notion.com/v1/users/me', headers={
        'Authorization': f'Bearer {notion_token}',
        'Notion-Version': '2022-06-28'
    })
    if resp.status_code != 200:
        logging.error(f"Notion token validation failed: {resp.status_code} {resp.text}")
        return jsonify({'error': 'Invalid Notion token. Make sure you copied the full token from your integration.'}), 400

    bot_info = resp.json()
    user_data = load_user(request.user_id)
    user_data['notion_token'] = notion_token
    user_data['notion_workspace'] = bot_info.get('name', 'Notion')
    save_user(user_data)

    logging.info(f"Notion token saved for user {request.user_id}, bot: {bot_info.get('name')}")
    return jsonify({'ok': True, 'bot_name': bot_info.get('name', 'Notion')})

@app.route('/api/notion/status', methods=['GET'])
@require_auth
def notion_status():
    """Check if Notion is connected and return database info"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    db_id = user_data.get('notion_database_id')
    db_name = user_data.get('notion_database_name')

    if not token:
        return jsonify({'connected': False})

    return jsonify({
        'connected': True,
        'workspace': user_data.get('notion_workspace', ''),
        'database_id': db_id,
        'database_name': db_name
    })

@app.route('/api/notion/disconnect', methods=['POST'])
@require_auth
def notion_disconnect():
    """Remove Notion connection"""
    user_data = load_user(request.user_id)
    user_data['notion_token'] = None
    user_data['notion_database_id'] = None
    user_data['notion_database_name'] = None
    user_data['notion_workspace'] = None
    user_data['notion_ingredients_db'] = None
    user_data['notion_recipes_db'] = None
    user_data['notion_recipe_items_db'] = None
    user_data['notion_recipe_page'] = None
    save_user(user_data)
    return jsonify({'ok': True})

@app.route('/api/notion/recipe-status', methods=['GET'])
@require_auth
def notion_recipe_status():
    """Check if Recipe Costing system is set up"""
    user_data = load_user(request.user_id)
    ingredients_db = user_data.get('notion_ingredients_db')
    recipes_db = user_data.get('notion_recipes_db')
    return jsonify({
        'setup': bool(ingredients_db and recipes_db),
        'ingredients_db': ingredients_db,
        'recipes_db': recipes_db,
        'recipe_items_db': user_data.get('notion_recipe_items_db')
    })

@app.route('/auth/notion/callback')
def notion_callback():
    """Handle Notion OAuth callback"""
    code = request.args.get('code')
    user_id = request.args.get('state')
    if not code or not user_id:
        return 'Missing code or state', 400

    # Exchange code for token
    resp = requests.post('https://api.notion.com/v1/oauth/token', json={
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': NOTION_REDIRECT_URI
    }, auth=(NOTION_CLIENT_ID, NOTION_CLIENT_SECRET))

    if resp.status_code != 200:
        return f'Notion auth failed: {resp.text}', 400

    token_data = resp.json()
    user_data = load_user(user_id)
    user_data['notion_token'] = token_data['access_token']
    user_data['notion_workspace'] = token_data.get('workspace_name', '')
    save_user(user_data)

    return redirect('/?notion=connected')

@app.route('/api/notion/databases', methods=['GET'])
@require_auth
def notion_databases():
    """List user's Notion databases to pick which one to sync to"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    if not token:
        return jsonify({'error': 'Notion not connected'}), 400

    resp = requests.post('https://api.notion.com/v1/search', json={
        'filter': {'property': 'object', 'value': 'database'}
    }, headers={
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2022-06-28'
    })

    if resp.status_code != 200:
        return jsonify({'error': 'Failed to fetch databases'}), 502

    databases = [{
        'id': db['id'],
        'title': db.get('title', [{}])[0].get('plain_text', 'Untitled') if db.get('title') else 'Untitled'
    } for db in resp.json().get('results', [])]

    return jsonify({'databases': databases})

@app.route('/api/notion/set-database', methods=['POST'])
@require_auth
def notion_set_database():
    """Set which Notion database to push invoices to"""
    data = request.json
    db_id = data.get('database_id')
    db_name = data.get('database_name', '')
    if not db_id:
        return jsonify({'error': 'Missing database_id'}), 400

    user_data = load_user(request.user_id)
    user_data['notion_database_id'] = db_id
    user_data['notion_database_name'] = db_name
    save_user(user_data)
    return jsonify({'ok': True})

@app.route('/api/notion/create-database', methods=['POST'])
@require_auth
def notion_create_database():
    """Create a RecipeSnap database in the user's Notion workspace"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    if not token:
        return jsonify({'error': 'Notion not connected'}), 400

    data = request.json or {}
    parent_page_id = data.get('page_id')

    if not parent_page_id:
        # Search for a page to put the database in
        # First try to find any page the integration has access to
        resp = requests.post('https://api.notion.com/v1/search', json={
            'filter': {'property': 'object', 'value': 'page'},
            'page_size': 10
        }, headers={
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2022-06-28'
        })
        if resp.status_code != 200:
            return jsonify({'error': 'Could not search Notion pages'}), 502
        pages = resp.json().get('results', [])
        if not pages:
            return jsonify({'error': 'No Notion pages found. Make sure your integration has access to at least one page.'}), 400
        parent_page_id = pages[0]['id']

    # Create the database with invoice-friendly properties
    db_payload = {
        'parent': {'type': 'page_id', 'page_id': parent_page_id},
        'title': [{'type': 'text', 'text': {'content': 'RecipeSnap Invoices'}}],
        'properties': {
            'Vendor': {'title': {}},
            'Invoice #': {'rich_text': {}},
            'Date': {'date': {}},
            'Due Date': {'date': {}},
            'Total': {'number': {'format': 'dollar'}},
            'Tax': {'number': {'format': 'dollar'}},
            'Subtotal': {'number': {'format': 'dollar'}},
            'Items Count': {'number': {'format': 'number'}},
            'Status': {'select': {'options': [
                {'name': 'Pending', 'color': 'yellow'},
                {'name': 'Reviewed', 'color': 'blue'},
                {'name': 'Approved', 'color': 'green'},
                {'name': 'Disputed', 'color': 'red'}
            ]}}
        }
    }

    resp = requests.post('https://api.notion.com/v1/databases', json=db_payload, headers={
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    })

    if resp.status_code != 200:
        logging.error(f"Notion create database error: {resp.status_code} {resp.text}")
        return jsonify({'error': f'Failed to create database: {resp.text}'}), 502

    db = resp.json()
    db_id = db['id']
    db_title = 'RecipeSnap Invoices'

    # Auto-save as the selected database
    user_data['notion_database_id'] = db_id
    user_data['notion_database_name'] = db_title
    save_user(user_data)

    logging.info(f"Created Notion database '{db_title}' ({db_id}) for user {request.user_id}")
    return jsonify({
        'ok': True,
        'database_id': db_id,
        'database_name': db_title,
        'notion_url': db.get('url', '')
    })

# ── Recipe Costing System ─────────────────────────────

def _notion_headers(token):
    return {
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }

def _notion_create_db(token, parent_page_id, title, properties):
    """Create a Notion database and return its ID"""
    payload = {
        'parent': {'type': 'page_id', 'page_id': parent_page_id},
        'title': [{'type': 'text', 'text': {'content': title}}],
        'properties': properties
    }
    resp = requests.post('https://api.notion.com/v1/databases',
                         json=payload, headers=_notion_headers(token))
    if resp.status_code != 200:
        logging.error(f"Create DB '{title}' failed: {resp.status_code} {resp.text}")
        return None, resp.text
    db = resp.json()
    return db['id'], db.get('url', '')

def _notion_update_db(token, db_id, properties):
    """Add/update properties on an existing Notion database"""
    resp = requests.patch(f'https://api.notion.com/v1/databases/{db_id}',
                          json={'properties': properties}, headers=_notion_headers(token))
    if resp.status_code != 200:
        logging.error(f"Update DB {db_id} failed: {resp.status_code} {resp.text}")
        return False
    return True

def _notion_append_blocks(token, page_id, blocks):
    """Append content blocks to a Notion page"""
    resp = requests.patch(f'https://api.notion.com/v1/blocks/{page_id}/children',
                          json={'children': blocks}, headers=_notion_headers(token))
    if resp.status_code != 200:
        logging.error(f"Append blocks to {page_id} failed: {resp.status_code} {resp.text}")
        return False
    return True

def _notion_create_page(token, db_id, properties, children=None):
    """Create a page in a Notion database"""
    payload = {
        'parent': {'database_id': db_id},
        'properties': properties
    }
    if children:
        payload['children'] = children
    resp = requests.post('https://api.notion.com/v1/pages',
                         json=payload, headers=_notion_headers(token))
    if resp.status_code != 200:
        logging.error(f"Create page failed: {resp.status_code} {resp.text}")
        return None
    return resp.json().get('id')

@app.route('/api/notion/create-recipe-system', methods=['POST'])
@require_auth
def notion_create_recipe_system():
    """Create the full Recipe Costing Calculator in Notion (3 databases + sample data)"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    if not token:
        return jsonify({'error': 'Notion not connected'}), 400

    # Find parent page the integration has access to
    resp = requests.post('https://api.notion.com/v1/search', json={
        'filter': {'property': 'object', 'value': 'page'},
        'page_size': 10
    }, headers=_notion_headers(token))
    if resp.status_code != 200:
        return jsonify({'error': 'Could not search Notion pages'}), 502
    pages = resp.json().get('results', [])
    if not pages:
        return jsonify({'error': 'No pages found. Share a Notion page with your integration first.'}), 400

    parent_page_id = pages[0]['id']

    # If recipe system already exists, clear old IDs and recreate
    if user_data.get('notion_ingredients_db'):
        user_data['notion_ingredients_db'] = None
        user_data['notion_recipes_db'] = None
        user_data['notion_recipe_items_db'] = None
        user_data['notion_recipe_page'] = None
        save_user(user_data)

    try:
        # ═══ STEP 1: Create parent page with dashboard layout ═══
        parent_resp = requests.post('https://api.notion.com/v1/pages', json={
            'parent': {'page_id': parent_page_id},
            'properties': {'title': {'title': [{'text': {'content': 'Recipe Costing Calculator'}}]}},
            'icon': {'type': 'emoji', 'emoji': '🍽️'},
            'cover': {'type': 'external', 'external': {'url': 'https://images.unsplash.com/photo-1556909114-f6e7ad7d3136?w=1200'}},
            'children': [
                # ── Hero callout ──
                {'object': 'block', 'type': 'callout', 'callout': {
                    'icon': {'type': 'emoji', 'emoji': '📊'},
                    'color': 'blue_background',
                    'rich_text': [
                        {'text': {'content': 'Recipe Costing Calculator\n'}, 'annotations': {'bold': True}},
                        {'text': {'content': 'Know your plate cost on every dish. Ingredient prices auto-update when you scan invoices with RecipeSnap — your food cost %, profit per plate, and recipe costs stay current without manual entry.'}}
                    ]
                }},
                {'object': 'block', 'type': 'divider', 'divider': {}},

                # ── How It Works section ──
                {'object': 'block', 'type': 'heading_2', 'heading_2': {
                    'rich_text': [{'text': {'content': '⚡ How It Works'}}]
                }},
                {'object': 'block', 'type': 'numbered_list_item', 'numbered_list_item': {
                    'rich_text': [
                        {'text': {'content': 'Scan an invoice'}, 'annotations': {'bold': True}},
                        {'text': {'content': ' — Take a photo with RecipeSnap. AI reads every line item, price, and quantity.'}}
                    ]
                }},
                {'object': 'block', 'type': 'numbered_list_item', 'numbered_list_item': {
                    'rich_text': [
                        {'text': {'content': 'Push to Notion'}, 'annotations': {'bold': True}},
                        {'text': {'content': ' — One tap sends the invoice here AND auto-updates your ingredient prices below.'}}
                    ]
                }},
                {'object': 'block', 'type': 'numbered_list_item', 'numbered_list_item': {
                    'rich_text': [
                        {'text': {'content': 'Costs update automatically'}, 'annotations': {'bold': True}},
                        {'text': {'content': ' — Every recipe recalculates: plate cost, food cost %, and profit per dish. No spreadsheets.'}}
                    ]
                }},
                {'object': 'block', 'type': 'paragraph', 'paragraph': {'rich_text': []}},

                # ── Recipes section header ──
                {'object': 'block', 'type': 'heading_2', 'heading_2': {
                    'rich_text': [{'text': {'content': '📋 Recipes'}}]
                }},
                {'object': 'block', 'type': 'paragraph', 'paragraph': {
                    'rich_text': [{'text': {'content': 'Each recipe pulls ingredient costs from your latest invoices. Add menu price and servings to see your margins.'}, 'annotations': {'italic': True, 'color': 'gray'}}]
                }},
            ]
        }, headers=_notion_headers(token))
        if parent_resp.status_code != 200:
            return jsonify({'error': f'Failed to create parent page: {parent_resp.text}'}), 502
        recipe_page_id = parent_resp.json()['id']
        logging.info(f"Created Recipe Costing page: {recipe_page_id}")

        # ═══ STEP 2: Create Ingredients database ═══
        ingredients_db_id, ingredients_url = _notion_create_db(token, recipe_page_id, 'Ingredients', {
            'Name': {'title': {}},
            'Vendor': {'rich_text': {}},
            'Category': {'select': {'options': [
                {'name': 'Produce', 'color': 'green'},
                {'name': 'Protein', 'color': 'red'},
                {'name': 'Seafood', 'color': 'blue'},
                {'name': 'Dairy', 'color': 'yellow'},
                {'name': 'Dry Goods', 'color': 'orange'},
                {'name': 'Beverages', 'color': 'purple'},
                {'name': 'Spices', 'color': 'brown'},
                {'name': 'Oils & Fats', 'color': 'default'},
                {'name': 'Bakery', 'color': 'pink'},
                {'name': 'Other', 'color': 'gray'}
            ]}},
            'Pack Description': {'rich_text': {}},
            'Pack Price': {'number': {'format': 'dollar'}},
            'Recipe Unit': {'select': {'options': [
                {'name': 'oz', 'color': 'blue'},
                {'name': 'lb', 'color': 'green'},
                {'name': 'each', 'color': 'orange'},
                {'name': 'fl oz', 'color': 'purple'},
                {'name': 'cup', 'color': 'yellow'},
                {'name': 'qt', 'color': 'pink'},
                {'name': 'gal', 'color': 'red'},
                {'name': 'bunch', 'color': 'default'}
            ]}},
            'Units per Pack': {'number': {'format': 'number'}},
            'Cost per Recipe Unit': {'formula': {
                'expression': 'if(prop("Units per Pack") > 0, round(prop("Pack Price") / prop("Units per Pack") * 100) / 100, if(prop("Yield (oz)") > 0, if(prop("Recipe Unit") == "lb", round(prop("Pack Price") / (prop("Yield (oz)") / 16) * 100) / 100, round(prop("Pack Price") / prop("Yield (oz)") * 100) / 100), 0))'
            }},
            'Last Invoice': {'rich_text': {}},
            'Last Updated': {'date': {}}
        })
        if not ingredients_db_id:
            return jsonify({'error': 'Failed to create Ingredients database'}), 502
        logging.info(f"Created Ingredients DB: {ingredients_db_id}")

        # Add section header before Recipes
        _notion_append_blocks(token, recipe_page_id, [
            {'object': 'block', 'type': 'paragraph', 'paragraph': {'rich_text': []}},
            {'object': 'block', 'type': 'heading_2', 'heading_2': {
                'rich_text': [{'text': {'content': '👨‍🍳 Your Recipes'}}]
            }},
            {'object': 'block', 'type': 'paragraph', 'paragraph': {
                'rich_text': [{'text': {'content': 'Add your menu items here. Set the menu price and servings — costs calculate automatically from your ingredients.'}, 'annotations': {'italic': True, 'color': 'gray'}}]
            }},
        ])

        # ═══ STEP 3: Create Recipes database ═══
        recipes_db_id, recipes_url = _notion_create_db(token, recipe_page_id, 'Recipes', {
            'Name': {'title': {}},
            'Category': {'select': {'options': [
                {'name': 'Appetizer', 'color': 'green'},
                {'name': 'Entrée', 'color': 'orange'},
                {'name': 'Side', 'color': 'yellow'},
                {'name': 'Dessert', 'color': 'pink'},
                {'name': 'Beverage', 'color': 'purple'},
                {'name': 'Cocktail', 'color': 'blue'},
                {'name': 'Sauce', 'color': 'red'},
                {'name': 'Special', 'color': 'default'}
            ]}},
            'Menu Price': {'number': {'format': 'dollar'}},
            'Servings': {'number': {'format': 'number'}},
            'Status': {'select': {'options': [
                {'name': 'Active', 'color': 'green'},
                {'name': 'Seasonal', 'color': 'blue'},
                {'name': 'Inactive', 'color': 'gray'},
                {'name': 'Testing', 'color': 'yellow'}
            ]}}
        })
        if not recipes_db_id:
            return jsonify({'error': 'Failed to create Recipes database'}), 502
        logging.info(f"Created Recipes DB: {recipes_db_id}")

        # Add section header before Recipe Items
        _notion_append_blocks(token, recipe_page_id, [
            {'object': 'block', 'type': 'paragraph', 'paragraph': {'rich_text': []}},
            {'object': 'block', 'type': 'heading_2', 'heading_2': {
                'rich_text': [{'text': {'content': '🧮 Recipe Items'}}]
            }},
            {'object': 'block', 'type': 'paragraph', 'paragraph': {
                'rich_text': [{'text': {'content': 'The breakdown of each recipe — what goes into each dish and how much it costs. This table links recipes to ingredients.'}, 'annotations': {'italic': True, 'color': 'gray'}}]
            }},
        ])

        # ═══ STEP 4: Create Recipe Items (junction table) with dual_property for bidirectional relation ═══
        recipe_items_db_id, ri_url = _notion_create_db(token, recipe_page_id, 'Recipe Items', {
            'Name': {'title': {}},
            'Recipe': {'relation': {
                'database_id': recipes_db_id,
                'dual_property': {'synced_property_name': 'Recipe Items'}
            }},
            'Ingredient': {'relation': {
                'database_id': ingredients_db_id,
                'single_property': {}
            }},
            'Qty Used': {'number': {'format': 'number'}},
            'Unit': {'select': {'options': [
                {'name': 'oz', 'color': 'blue'},
                {'name': 'lb', 'color': 'green'},
                {'name': 'each', 'color': 'orange'},
                {'name': 'fl oz', 'color': 'purple'},
                {'name': 'cup', 'color': 'yellow'},
                {'name': 'qt', 'color': 'pink'},
                {'name': 'gal', 'color': 'red'},
                {'name': 'bunch', 'color': 'default'}
            ]}},
        })
        if not recipe_items_db_id:
            return jsonify({'error': 'Failed to create Recipe Items database'}), 502
        logging.info(f"Created Recipe Items DB: {recipe_items_db_id}")

        # ═══ STEP 5a: Add Unit Cost rollup to Recipe Items ═══
        time.sleep(1)
        _notion_update_db(token, recipe_items_db_id, {
            'Unit Cost': {'rollup': {
                'relation_property_name': 'Ingredient',
                'rollup_property_name': 'Cost per Recipe Unit',
                'function': 'sum'
            }}
        })

        # ═══ STEP 5b: Add Line Cost formula (depends on Unit Cost rollup) ═══
        time.sleep(1)
        _notion_update_db(token, recipe_items_db_id, {
            'Line Cost': {'formula': {
                'expression': 'round(prop("Qty Used") * prop("Unit Cost") * 100) / 100'
            }}
        })

        # ═══ STEP 6a: Add Total Cost rollup to Recipes ═══
        time.sleep(2)
        _notion_update_db(token, recipes_db_id, {
            'Total Cost': {'rollup': {
                'relation_property_name': 'Recipe Items',
                'rollup_property_name': 'Line Cost',
                'function': 'sum'
            }}
        })

        # ═══ STEP 6b: Add Cost per Serving as formatted string ($X.XX) ═══
        time.sleep(1)
        _notion_update_db(token, recipes_db_id, {
            'Cost per Serving': {'formula': {
                'expression': 'if(prop("Servings") > 0, "$" + format(round(prop("Total Cost") / prop("Servings") * 100) / 100), "$0.00")'
            }}
        })

        # ═══ STEP 6c: Add Food Cost Pct as formatted string (X.X%) ═══
        time.sleep(3)
        result_fc = _notion_update_db(token, recipes_db_id, {
            'Food Cost Pct': {'formula': {
                'expression': 'if(prop("Servings") > 0, if(prop("Menu Price") > 0, format(round(prop("Total Cost") / prop("Servings") / prop("Menu Price") * 1000) / 10) + "%", "0%"), "0%")'
            }}
        })
        logging.info(f"Step 6c Food Cost Pct: {result_fc}")

        # ═══ STEP 6d: Add Profit per Plate as formatted string ($X.XX) ═══
        time.sleep(2)
        result_pp = _notion_update_db(token, recipes_db_id, {
            'Profit per Plate': {'formula': {
                'expression': 'if(prop("Servings") > 0, "$" + format(round((prop("Menu Price") - prop("Total Cost") / prop("Servings")) * 100) / 100), "$0.00")'
            }}
        })
        logging.info(f"Step 6d Profit per Plate: {result_pp}")

        # ═══ STEP 7: Add sample ingredients ═══
        sample_ingredients = [
            {'name': 'Lobster Meat', 'vendor': 'Cape Cod Shellfish', 'cat': 'Seafood', 'pack': '2 lb pack', 'price': 29.50, 'unit': 'lb', 'per_pack': 2},
            {'name': 'Cavatappi Pasta', 'vendor': 'US Foods', 'cat': 'Dry Goods', 'pack': '10 lb case', 'price': 12.80, 'unit': 'lb', 'per_pack': 10},
            {'name': 'Sharp Cheddar', 'vendor': 'Performance Food', 'cat': 'Dairy', 'pack': '5 lb bag', 'price': 18.40, 'unit': 'lb', 'per_pack': 5},
            {'name': 'Heavy Cream', 'vendor': 'Performance Food', 'cat': 'Dairy', 'pack': '1 qt', 'price': 5.20, 'unit': 'qt', 'per_pack': 1},
            {'name': 'Gruyère', 'vendor': 'Performance Food', 'cat': 'Dairy', 'pack': '1 lb block', 'price': 12.60, 'unit': 'lb', 'per_pack': 1},
            {'name': 'Butter', 'vendor': 'US Foods', 'cat': 'Dairy', 'pack': '1 lb', 'price': 4.80, 'unit': 'lb', 'per_pack': 1},
            {'name': 'Panko Breadcrumbs', 'vendor': 'US Foods', 'cat': 'Dry Goods', 'pack': '3 lb bag', 'price': 7.50, 'unit': 'lb', 'per_pack': 3},
            {'name': 'Old Bay Seasoning', 'vendor': 'US Foods', 'cat': 'Spices', 'pack': '24 oz can', 'price': 8.90, 'unit': 'oz', 'per_pack': 24},
            {'name': 'Iceberg Lettuce', 'vendor': 'Performance Food', 'cat': 'Produce', 'pack': '24 ct case', 'price': 28.50, 'unit': 'oz', 'per_pack': 384},
            {'name': 'Chicken Breast', 'vendor': 'US Foods', 'cat': 'Protein', 'pack': '40 lb case', 'price': 85.00, 'unit': 'oz', 'per_pack': 640},
            {'name': 'Roma Tomatoes', 'vendor': 'Performance Food', 'cat': 'Produce', 'pack': '25 lb case', 'price': 22.00, 'unit': 'oz', 'per_pack': 400},
            {'name': 'Olive Oil', 'vendor': 'US Foods', 'cat': 'Oils & Fats', 'pack': '1 gal jug', 'price': 35.00, 'unit': 'fl oz', 'per_pack': 128},
        ]

        ingredient_ids = {}
        for ing in sample_ingredients:
            page_id = _notion_create_page(token, ingredients_db_id, {
                'Name': {'title': [{'text': {'content': ing['name']}}]},
                'Vendor': {'rich_text': [{'text': {'content': ing['vendor']}}]},
                'Category': {'select': {'name': ing['cat']}},
                'Pack Description': {'rich_text': [{'text': {'content': ing['pack']}}]},
                'Pack Price': {'number': ing['price']},
                'Recipe Unit': {'select': {'name': ing['unit']}},
                'Units per Pack': {'number': ing['per_pack']},
                'Last Updated': {'date': {'start': datetime.now().strftime('%Y-%m-%d')}}
            })
            if page_id:
                ingredient_ids[ing['name']] = page_id
                logging.info(f"Created ingredient: {ing['name']} -> {page_id}")

        # ═══ STEP 8: Create sample recipe — Lobster Mac & Cheese ═══
        recipe_id = _notion_create_page(token, recipes_db_id, {
            'Name': {'title': [{'text': {'content': 'Lobster Mac & Cheese'}}]},
            'Category': {'select': {'name': 'Entrée'}},
            'Menu Price': {'number': 26.00},
            'Servings': {'number': 1},
            'Status': {'select': {'name': 'Active'}}
        })
        logging.info(f"Created recipe: Lobster Mac & Cheese -> {recipe_id}")

        # ═══ STEP 9: Add recipe items for Lobster Mac & Cheese ═══
        recipe_items_data = [
            {'name': 'Lobster Meat', 'qty': 0.33, 'unit': 'lb'},
            {'name': 'Cavatappi Pasta', 'qty': 0.50, 'unit': 'lb'},
            {'name': 'Sharp Cheddar', 'qty': 0.25, 'unit': 'lb'},
            {'name': 'Heavy Cream', 'qty': 0.25, 'unit': 'qt'},
            {'name': 'Gruyère', 'qty': 0.04, 'unit': 'lb'},
            {'name': 'Butter', 'qty': 0.06, 'unit': 'lb'},
            {'name': 'Panko Breadcrumbs', 'qty': 0.02, 'unit': 'lb'},
            {'name': 'Old Bay Seasoning', 'qty': 0.10, 'unit': 'oz'},
        ]

        for ri in recipe_items_data:
            ing_id = ingredient_ids.get(ri['name'])
            if not ing_id or not recipe_id:
                continue
            _notion_create_page(token, recipe_items_db_id, {
                'Name': {'title': [{'text': {'content': ri['name']}}]},
                'Recipe': {'relation': [{'id': recipe_id}]},
                'Ingredient': {'relation': [{'id': ing_id}]},
                'Qty Used': {'number': ri['qty']},
                'Unit': {'select': {'name': ri['unit']}}
            })

        # ═══ STEP 10: Create second sample recipe — Chicken Parm ═══
        recipe2_id = _notion_create_page(token, recipes_db_id, {
            'Name': {'title': [{'text': {'content': 'Chicken Parmesan'}}]},
            'Category': {'select': {'name': 'Entrée'}},
            'Menu Price': {'number': 22.00},
            'Servings': {'number': 1},
            'Status': {'select': {'name': 'Active'}}
        })

        chicken_parm_items = [
            {'name': 'Chicken Breast', 'qty': 8, 'unit': 'oz'},
            {'name': 'Cavatappi Pasta', 'qty': 0.38, 'unit': 'lb'},
            {'name': 'Sharp Cheddar', 'qty': 0.19, 'unit': 'lb'},
            {'name': 'Olive Oil', 'qty': 1, 'unit': 'fl oz'},
            {'name': 'Roma Tomatoes', 'qty': 6, 'unit': 'oz'},
            {'name': 'Butter', 'qty': 0.03, 'unit': 'lb'},
        ]

        for ri in chicken_parm_items:
            ing_id = ingredient_ids.get(ri['name'])
            if not ing_id or not recipe2_id:
                continue
            _notion_create_page(token, recipe_items_db_id, {
                'Name': {'title': [{'text': {'content': ri['name']}}]},
                'Recipe': {'relation': [{'id': recipe2_id}]},
                'Ingredient': {'relation': [{'id': ing_id}]},
                'Qty Used': {'number': ri['qty']},
                'Unit': {'select': {'name': ri['unit']}}
            })

        # ═══ Add tips section at the bottom ═══
        _notion_append_blocks(token, recipe_page_id, [
            {'object': 'block', 'type': 'divider', 'divider': {}},
            {'object': 'block', 'type': 'heading_2', 'heading_2': {
                'rich_text': [{'text': {'content': '💡 Quick Tips'}}]
            }},
            {'object': 'block', 'type': 'bulleted_list_item', 'bulleted_list_item': {
                'rich_text': [
                    {'text': {'content': 'Target food cost under 30%'}, 'annotations': {'bold': True}},
                    {'text': {'content': ' — Most restaurants aim for 28-32%. Anything over 35% needs attention.'}}
                ]
            }},
            {'object': 'block', 'type': 'bulleted_list_item', 'bulleted_list_item': {
                'rich_text': [
                    {'text': {'content': 'Scan every invoice'}, 'annotations': {'bold': True}},
                    {'text': {'content': ' — The more invoices you push, the more accurate your costs. Prices update automatically.'}}
                ]
            }},
            {'object': 'block', 'type': 'bulleted_list_item', 'bulleted_list_item': {
                'rich_text': [
                    {'text': {'content': 'Check "Recipe Items" quantities'}, 'annotations': {'bold': True}},
                    {'text': {'content': ' — Make sure Qty Used matches your actual portions. This is the key to accurate costing.'}}
                ]
            }},
            {'object': 'block', 'type': 'bulleted_list_item', 'bulleted_list_item': {
                'rich_text': [
                    {'text': {'content': 'Sample data included'}, 'annotations': {'bold': True}},
                    {'text': {'content': ' — Lobster Mac & Cheese and Chicken Parmesan are examples. Edit or delete them and add your own recipes.'}}
                ]
            }},
            {'object': 'block', 'type': 'paragraph', 'paragraph': {'rich_text': []}},
            {'object': 'block', 'type': 'callout', 'callout': {
                'icon': {'type': 'emoji', 'emoji': '🔗'},
                'color': 'gray_background',
                'rich_text': [{'text': {'content': 'Powered by RecipeSnap — AI invoice scanning for restaurants. Scan invoices, track costs, protect your margins.'}}]
            }},
        ])

        # ═══ Save database IDs for future use ═══
        user_data['notion_ingredients_db'] = ingredients_db_id
        user_data['notion_recipes_db'] = recipes_db_id
        user_data['notion_recipe_items_db'] = recipe_items_db_id
        user_data['notion_recipe_page'] = recipe_page_id
        save_user(user_data)

        logging.info(f"Recipe Costing system created for user {request.user_id}")
        return jsonify({
            'ok': True,
            'ingredients_db': ingredients_db_id,
            'recipes_db': recipes_db_id,
            'recipe_items_db': recipe_items_db_id,
        })

    except Exception as e:
        logging.error(f"Recipe system creation error: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/notion/push', methods=['POST'])
@require_auth
def notion_push():
    """Push a scanned invoice to the user's Notion database"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    db_id = user_data.get('notion_database_id')

    if not token or not db_id:
        return jsonify({'error': 'Notion not connected or no database selected'}), 400

    invoice = request.json.get('invoice')
    push_status = request.json.get('status', 'Pending')  # Frontend sends Reviewed or Pending
    if not invoice:
        return jsonify({'error': 'No invoice data'}), 400

    # Validate status
    if push_status not in ('Pending', 'Reviewed', 'Approved', 'Disputed'):
        push_status = 'Pending'

    # Build Notion page with invoice data
    items = invoice.get('items', [])
    items_sum = round(sum(i.get('total_price', 0) for i in items), 2)
    properties = {
        'Vendor': {'title': [{'text': {'content': invoice.get('vendor_name', 'Unknown')}}]},
        'Invoice #': {'rich_text': [{'text': {'content': str(invoice.get('invoice_number', ''))}}]},
        'Date': {'date': {'start': _parse_date(invoice.get('invoice_date', ''))}},
        'Total': {'number': invoice.get('total', 0)},
        'Tax': {'number': invoice.get('tax', 0) or 0},
        'Subtotal': {'number': items_sum},
        'Items Count': {'number': len(items)},
        'Status': {'select': {'name': push_status}},
    }

    if invoice.get('due_date'):
        properties['Due Date'] = {'date': {'start': _parse_date(invoice['due_date'])}}

    # Build line items as page content (children blocks)
    children = []
    children.append({
        'object': 'block',
        'type': 'heading_3',
        'heading_3': {'rich_text': [{'text': {'content': 'Line Items'}}]}
    })

    # Table block for items
    if items:
        children.append({
            'object': 'block',
            'type': 'table',
            'table': {
                'table_width': 5,
                'has_column_header': True,
                'has_row_header': False,
                'children': [
                    # Header row
                    {'type': 'table_row', 'table_row': {'cells': [
                        [{'text': {'content': 'Item'}}],
                        [{'text': {'content': 'Pack/Size'}}],
                        [{'text': {'content': 'Qty'}}],
                        [{'text': {'content': 'Unit Price'}}],
                        [{'text': {'content': 'Total'}}],
                    ]}}
                ] + [
                    # Data rows
                    {'type': 'table_row', 'table_row': {'cells': [
                        [{'text': {'content': item['item_name'][:100]}}],
                        [{'text': {'content': str(item.get('pack_size', '') or '')[:50]}}],
                        [{'text': {'content': str(item['quantity'])}}],
                        [{'text': {'content': f"${item['unit_price']:.2f}"}}],
                        [{'text': {'content': f"${item['total_price']:.2f}"}}],
                    ]}} for item in items[:98]  # Notion limit: 100 blocks per request
                ]
            }
        })

    resp = requests.post('https://api.notion.com/v1/pages', json={
        'parent': {'database_id': db_id},
        'properties': properties,
        'children': children
    }, headers={
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    })

    if resp.status_code != 200:
        return jsonify({'error': f'Notion API error: {resp.text}'}), 502

    page = resp.json()

    # ── Auto-update Ingredients database if recipe system is set up ──
    ingredients_db = user_data.get('notion_ingredients_db')
    if ingredients_db and items:
        try:
            _update_ingredients_from_invoice(token, ingredients_db, items, invoice)
        except Exception as e:
            logging.error(f"Ingredient update error (non-fatal): {e}")

    # ── Auto-resolve canonical names so recipe costs update ──
    recipe_items_db = user_data.get('notion_recipe_items_db')
    if ingredients_db and recipe_items_db:
        try:
            _resolve_canonical_names(token, ingredients_db, recipe_items_db)
        except Exception as e:
            logging.error(f"Canonical resolve error (non-fatal): {e}")

    return jsonify({
        'ok': True,
        'notion_url': page.get('url', ''),
        'page_id': page.get('id', '')
    })

def _update_ingredients_from_invoice(token, ingredients_db_id, items, invoice):
    """When an invoice is pushed, update matching ingredients with new prices"""
    vendor = invoice.get('vendor_name', '')
    inv_num = invoice.get('invoice_number', '')

    for item in items:
        item_name = item.get('item_name', '').strip()
        if not item_name or item.get('total_price', 0) <= 0:
            continue

        # Search for existing ingredient by name
        search_resp = requests.post('https://api.notion.com/v1/databases/' + ingredients_db_id + '/query',
            json={'filter': {'property': 'Name', 'title': {'equals': item_name}}},
            headers=_notion_headers(token))

        if search_resp.status_code != 200:
            continue

        results = search_resp.json().get('results', [])
        unit_price = item.get('unit_price', 0)
        qty = item.get('quantity', 1)
        # Pack Price = price of ONE case/pack, not the line total
        pack_price = unit_price if unit_price > 0 else item.get('total_price', 0)

        pack_size_str = item.get('pack_size', '')
        pack_weight_oz = item.get('pack_weight_oz')

        if results:
            # Update existing ingredient's price
            page_id = results[0]['id']
            update_props = {
                'Pack Price': {'number': pack_price},
                'Vendor': {'rich_text': [{'text': {'content': vendor}}]},
                'Last Invoice': {'rich_text': [{'text': {'content': inv_num}}]},
                'Last Updated': {'date': {'start': datetime.now().strftime('%Y-%m-%d')}}
            }
            if pack_size_str:
                update_props['Pack Description'] = {'rich_text': [{'text': {'content': pack_size_str}}]}
                update_props['Pack Size'] = {'rich_text': [{'text': {'content': pack_size_str}}]}
            if pack_weight_oz and isinstance(pack_weight_oz, (int, float)) and pack_weight_oz > 0:
                update_props['Yield (oz)'] = {'number': pack_weight_oz}

            requests.patch(f'https://api.notion.com/v1/pages/{page_id}',
                json={'properties': update_props}, headers=_notion_headers(token))
            logging.info(f"Updated ingredient '{item_name}' price -> ${pack_price}")
        else:
            # Create new ingredient entry
            create_props = {
                'Name': {'title': [{'text': {'content': item_name}}]},
                'Vendor': {'rich_text': [{'text': {'content': vendor}}]},
                'Pack Price': {'number': pack_price},
                'Pack Description': {'rich_text': [{'text': {'content': pack_size_str or f'{qty} x ${unit_price}'}}]},
                'Pack Size': {'rich_text': [{'text': {'content': pack_size_str}}]},
                'Last Invoice': {'rich_text': [{'text': {'content': inv_num}}]},
                'Last Updated': {'date': {'start': datetime.now().strftime('%Y-%m-%d')}}
            }
            if pack_weight_oz and isinstance(pack_weight_oz, (int, float)) and pack_weight_oz > 0:
                create_props['Yield (oz)'] = {'number': pack_weight_oz}
            _notion_create_page(token, ingredients_db_id, create_props)
            logging.info(f"Created new ingredient from invoice: '{item_name}'")


# ── Routes: Upgrade Ingredients DB with new properties ───
@app.route('/api/recipe/upgrade-ingredients', methods=['POST'])
@require_auth
def upgrade_ingredients_db():
    """Add Canonical Name, Pack Size, Yield (oz), Cost Per Oz to Ingredients DB"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    ingredients_db = user_data.get('notion_ingredients_db')

    if not token or not ingredients_db:
        return jsonify({'error': 'Notion not connected or Ingredients DB not set up'}), 400

    # Add new properties to the database schema
    new_props = {
        'Canonical Name': {'select': {'options': []}},
        'Pack Size': {'rich_text': {}},
        'Yield (oz)': {'number': {'format': 'number'}},
    }

    resp = requests.patch(f'https://api.notion.com/v1/databases/{ingredients_db}',
        json={'properties': new_props},
        headers=_notion_headers(token))

    if resp.status_code != 200:
        return jsonify({'error': f'Failed to update DB schema: {resp.text}'}), 502

    # Now add the formula property (Cost Per Oz = Pack Price / Yield)
    formula_props = {
        'Cost Per Oz': {
            'formula': {
                'expression': 'if(prop("Yield (oz)") > 0, prop("Pack Price") / prop("Yield (oz)"), 0)'
            }
        }
    }
    resp2 = requests.patch(f'https://api.notion.com/v1/databases/{ingredients_db}',
        json={'properties': formula_props},
        headers=_notion_headers(token))

    if resp2.status_code != 200:
        logging.warning(f"Formula property failed (non-fatal): {resp2.text}")
        return jsonify({'ok': True, 'formula_added': False, 'note': 'Base properties added but formula failed — add manually'})

    # Fix "Cost per Unit" formula — uses Recipe Unit + Yield (oz) to calculate accurate cost
    # Priority: 1) Manual Units per Pack if set, 2) Auto-convert Yield (oz) based on Recipe Unit
    # Recipe Unit = oz → divide by Yield(oz)
    # Recipe Unit = lb → divide by Yield(oz)/16
    # Recipe Unit = each → must use Units per Pack (can't derive count from weight)
    # Recipe Unit = gal → divide by Yield(oz)/128
    # Recipe Unit = qt → divide by Yield(oz)/32
    # Recipe Unit = cup → divide by Yield(oz)/8
    # Recipe Unit = fl oz → divide by Yield(oz) (same as oz for liquid)
    cpu_expression = (
        'if(prop("Units per Pack") > 0, '
        'round(prop("Pack Price") / prop("Units per Pack") * 100) / 100, '
        'if(prop("Yield (oz)") > 0, '
        'if(prop("Recipe Unit") == "lb", '
        'round(prop("Pack Price") / (prop("Yield (oz)") / 16) * 100) / 100, '
        'if(prop("Recipe Unit") == "gal", '
        'round(prop("Pack Price") / (prop("Yield (oz)") / 128) * 100) / 100, '
        'if(prop("Recipe Unit") == "qt", '
        'round(prop("Pack Price") / (prop("Yield (oz)") / 32) * 100) / 100, '
        'if(prop("Recipe Unit") == "cup", '
        'round(prop("Pack Price") / (prop("Yield (oz)") / 8) * 100) / 100, '
        'round(prop("Pack Price") / prop("Yield (oz)") * 100) / 100)))), '
        '0))'
    )
    cpu_formula = {
        'Cost per Unit': {
            'name': 'Cost per Recipe Unit',
            'formula': {
                'expression': cpu_expression
            }
        }
    }
    resp_cpu = requests.patch(f'https://api.notion.com/v1/databases/{ingredients_db}',
        json={'properties': cpu_formula},
        headers=_notion_headers(token))
    cpu_fixed = resp_cpu.status_code == 200
    if not cpu_fixed:
        logging.warning(f"Cost per Recipe Unit formula update failed: {resp_cpu.text}")

    # Also rename the rollup on Recipe Items to match
    recipe_items_db_tmp = user_data.get('notion_recipe_items_db')
    if recipe_items_db_tmp and cpu_fixed:
        requests.patch(f'https://api.notion.com/v1/databases/{recipe_items_db_tmp}',
            json={'properties': {'Unit Cost': {'name': 'Unit Cost'}}},
            headers=_notion_headers(token))

    # Also add Canonical Name to Recipe Items DB so recipes can reference canonical names
    recipe_items_db = user_data.get('notion_recipe_items_db')
    if recipe_items_db:
        resp3 = requests.patch(f'https://api.notion.com/v1/databases/{recipe_items_db}',
            json={'properties': {'Canonical Name': {'select': {'options': []}}}},
            headers=_notion_headers(token))
        if resp3.status_code != 200:
            logging.warning(f"Recipe Items Canonical Name failed (non-fatal): {resp3.text}")

    return jsonify({'ok': True, 'formula_added': True, 'cost_per_unit_fixed': cpu_fixed, 'properties_added': ['Canonical Name', 'Pack Size', 'Yield (oz)', 'Cost Per Oz', 'Cost per Unit (updated)', 'Canonical Name (Recipe Items)']})


# ── Routes: List Canonical Names ──────────────────────────
@app.route('/api/recipe/canonical-names', methods=['GET'])
@require_auth
def list_canonical_names():
    """List all canonical names and their ingredient counts"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    ingredients_db = user_data.get('notion_ingredients_db')

    if not token or not ingredients_db:
        return jsonify({'error': 'Not set up'}), 400

    canonical = {}
    has_more = True
    start_cursor = None
    while has_more:
        body = {'page_size': 100, 'filter': {
            'property': 'Canonical Name',
            'select': {'is_not_empty': True}
        }}
        if start_cursor:
            body['start_cursor'] = start_cursor
        resp = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
            json=body, headers=_notion_headers(token))
        if resp.status_code != 200:
            break
        qdata = resp.json()
        for page in qdata.get('results', []):
            cn = page.get('properties', {}).get('Canonical Name', {}).get('select', {})
            if not cn:
                continue
            name = cn.get('name', '')
            item_name = ''
            title = page.get('properties', {}).get('Name', {}).get('title', [])
            if title:
                item_name = ''.join(t.get('plain_text', '') for t in title)
            if name not in canonical:
                canonical[name] = []
            canonical[name].append(item_name)
        has_more = qdata.get('has_more', False)
        start_cursor = qdata.get('next_cursor')

    return jsonify({'ok': True, 'canonical_names': canonical})


# ── Routes: All Ingredients (canonical + raw) ────────────
@app.route('/api/recipe/all-ingredients', methods=['GET'])
@require_auth
def list_all_ingredients():
    """Return every ingredient: canonical names (deduplicated) + raw names without a canonical."""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    ingredients_db = user_data.get('notion_ingredients_db')

    if not token or not ingredients_db:
        return jsonify({'error': 'Not set up'}), 400

    canonical_set = set()   # track canonical names we've seen
    raw_list = []           # raw names without a canonical
    has_more = True
    start_cursor = None
    while has_more:
        body = {'page_size': 100}
        if start_cursor:
            body['start_cursor'] = start_cursor
        resp = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
            json=body, headers=_notion_headers(token))
        if resp.status_code != 200:
            break
        qdata = resp.json()
        for page in qdata.get('results', []):
            props = page.get('properties', {})
            # Get raw name
            title = props.get('Name', {}).get('title', [])
            raw_name = ''.join(t.get('plain_text', '') for t in title) if title else ''
            # Get canonical name
            cn = props.get('Canonical Name', {}).get('select', {})
            cn_name = cn.get('name', '') if cn else ''
            if cn_name:
                canonical_set.add(cn_name)
            elif raw_name:
                raw_list.append(raw_name)
        has_more = qdata.get('has_more', False)
        start_cursor = qdata.get('next_cursor')

    # Build combined list: canonical first, then raw (no duplicates)
    ingredients = []
    for cn in sorted(canonical_set):
        ingredients.append({'name': cn, 'type': 'canonical'})
    for rn in sorted(raw_list):
        ingredients.append({'name': rn, 'type': 'raw'})

    return jsonify({'ok': True, 'ingredients': ingredients})


# ── Routes: Find Recipe by Name ──────────────────────────
@app.route('/api/recipe/find', methods=['GET'])
@require_auth
def find_recipe():
    """Find a recipe by name. Usage: /api/recipe/find?name=BYO+Burger"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    recipes_db = user_data.get('notion_recipes_db')

    if not token or not recipes_db:
        return jsonify({'error': 'Not set up'}), 400

    name = request.args.get('name', '')
    if not name:
        return jsonify({'error': 'name parameter required'}), 400

    resp = requests.post(f'https://api.notion.com/v1/databases/{recipes_db}/query',
        json={'filter': {'property': 'Name', 'title': {'contains': name}}},
        headers=_notion_headers(token))

    if resp.status_code != 200:
        return jsonify({'error': f'Query failed: {resp.text}'}), 502

    results = resp.json().get('results', [])
    recipes = []
    for r in results:
        title = r.get('properties', {}).get('Name', {}).get('title', [])
        rname = ''.join(t.get('plain_text', '') for t in title) if title else ''
        recipes.append({'id': r['id'], 'name': rname})

    return jsonify({'ok': True, 'recipes': recipes})


# ── Routes: Recipe Details ────────────────────────────────
@app.route('/api/recipe/details', methods=['GET'])
@require_auth
def recipe_details():
    """Get recipe details + its items with costs"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    recipes_db = user_data.get('notion_recipes_db')
    recipe_items_db = user_data.get('notion_recipe_items_db')

    if not token or not recipes_db or not recipe_items_db:
        return jsonify({'error': 'Not set up'}), 400

    recipe_id = request.args.get('id', '')
    if not recipe_id:
        return jsonify({'error': 'id parameter required'}), 400

    # Get recipe page
    resp = requests.get(f'https://api.notion.com/v1/pages/{recipe_id}',
        headers=_notion_headers(token))
    if resp.status_code != 200:
        return jsonify({'error': 'Recipe not found'}), 404

    page = resp.json()
    props = page.get('properties', {})

    name = ''.join(t.get('plain_text', '') for t in props.get('Name', {}).get('title', []))
    category = props.get('Category', {}).get('select', {})
    category_name = category.get('name', '') if category else ''
    menu_price = props.get('Menu Price', {}).get('number') or 0
    servings = props.get('Servings', {}).get('number') or 1

    # Get formula results
    food_cost_pct = ''
    fc_prop = props.get('Food Cost Pct', {}).get('formula', {})
    if fc_prop.get('string'):
        food_cost_pct = fc_prop['string']
    elif fc_prop.get('number') is not None:
        food_cost_pct = f"{fc_prop['number']:.1f}%"

    profit_per_plate = ''
    pp_prop = props.get('Profit per Plate', {}).get('formula', {})
    if pp_prop.get('string'):
        profit_per_plate = pp_prop['string']
    elif pp_prop.get('number') is not None:
        profit_per_plate = f"${pp_prop['number']:.2f}"

    cost_per_serving = ''
    cps_prop = props.get('Cost per Serving', {}).get('formula', {})
    if cps_prop.get('string'):
        cost_per_serving = cps_prop['string']
    elif cps_prop.get('number') is not None:
        cost_per_serving = f"${cps_prop['number']:.2f}"

    # Get recipe items
    items = []
    ri_resp = requests.post(f'https://api.notion.com/v1/databases/{recipe_items_db}/query',
        json={'filter': {'property': 'Recipe', 'relation': {'contains': recipe_id}}},
        headers=_notion_headers(token))

    if ri_resp.status_code == 200:
        for ri in ri_resp.json().get('results', []):
            ri_props = ri.get('properties', {})
            ri_name = ''.join(t.get('plain_text', '') for t in ri_props.get('Name', {}).get('title', []))
            qty = ri_props.get('Qty Used', {}).get('number') or 0
            unit = ri_props.get('Unit', {}).get('select', {})
            unit_name = unit.get('name', '') if unit else ''

            # Get Line Cost from formula
            line_cost = None
            lc_prop = ri_props.get('Line Cost', {}).get('formula', {})
            if lc_prop.get('number') is not None:
                line_cost = lc_prop['number']

            items.append({
                'id': ri['id'],
                'name': ri_name,
                'qty': qty,
                'unit': unit_name,
                'line_cost': line_cost
            })

    return jsonify({
        'ok': True,
        'name': name,
        'category': category_name,
        'menu_price': menu_price,
        'servings': servings,
        'food_cost_pct': food_cost_pct,
        'profit_per_plate': profit_per_plate,
        'cost_per_serving': cost_per_serving,
        'items': items
    })


# ── Routes: Ingredient Info ──────────────────────────────
@app.route('/api/recipe/ingredient-info', methods=['GET'])
@require_auth
def ingredient_info():
    """Get an ingredient's recipe unit and cost per unit by canonical or raw name"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    ingredients_db = user_data.get('notion_ingredients_db')

    if not token or not ingredients_db:
        return jsonify({'error': 'Not set up'}), 400

    name = request.args.get('name', '')
    if not name:
        return jsonify({'error': 'name required'}), 400

    # Try canonical name first
    resp = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
        json={'filter': {'property': 'Canonical Name', 'select': {'equals': name}}},
        headers=_notion_headers(token))

    results = []
    if resp.status_code == 200:
        results = resp.json().get('results', [])

    # Fall back to raw name search
    if not results:
        resp2 = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
            json={'filter': {'property': 'Name', 'title': {'equals': name}}},
            headers=_notion_headers(token))
        if resp2.status_code == 200:
            results = resp2.json().get('results', [])

    if not results:
        return jsonify({'ok': True, 'recipe_unit': 'oz', 'cost_per_unit': 0})

    # Use most recently updated
    best = results[0]
    for r in results[1:]:
        r_date = r.get('properties', {}).get('Last Updated', {}).get('date', {})
        b_date = best.get('properties', {}).get('Last Updated', {}).get('date', {})
        if r_date and b_date and (r_date.get('start', '') > b_date.get('start', '')):
            best = r

    props = best.get('properties', {})
    ru = props.get('Recipe Unit', {}).get('select', {})
    recipe_unit = ru.get('name', 'oz') if ru else 'oz'

    cost_per_unit = 0
    cpu_prop = props.get('Cost per Recipe Unit', {}).get('formula', {})
    if cpu_prop.get('number') is not None:
        cost_per_unit = cpu_prop['number']
    # Fall back to old name
    if cost_per_unit == 0:
        cpu_prop2 = props.get('Cost per Unit', {}).get('formula', {})
        if cpu_prop2 and cpu_prop2.get('number') is not None:
            cost_per_unit = cpu_prop2['number']

    return jsonify({
        'ok': True,
        'recipe_unit': recipe_unit,
        'cost_per_unit': cost_per_unit,
        'pack_price': props.get('Pack Price', {}).get('number') or 0
    })


# ── Routes: Create Recipe ────────────────────────────────
@app.route('/api/recipe/create', methods=['POST'])
@require_auth
def create_recipe():
    """Create a new recipe"""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    recipes_db = user_data.get('notion_recipes_db')

    if not token or not recipes_db:
        return jsonify({'error': 'Not set up'}), 400

    data = request.json or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name is required'}), 400

    menu_price = data.get('menu_price', 0)
    servings = data.get('servings', 1)
    category = data.get('category', 'Entrée')

    page_id = _notion_create_page(token, recipes_db, {
        'Name': {'title': [{'text': {'content': name}}]},
        'Category': {'select': {'name': category}},
        'Menu Price': {'number': menu_price},
        'Servings': {'number': servings},
        'Status': {'select': {'name': 'Active'}}
    })

    if not page_id:
        return jsonify({'error': 'Failed to create recipe'}), 502

    return jsonify({'ok': True, 'recipe_id': page_id})


# ── Routes: Add Recipe Items by Canonical Name ───────────
@app.route('/api/recipe/add-items', methods=['POST'])
@require_auth
def add_recipe_items():
    """Add items to a recipe using canonical names OR raw ingredient names.
    Body: {recipe_id: "...", items: [{name: "Burger Patty", qty: 8, unit: "oz"}, ...]}
    Lookup order: 1) Canonical Name match, 2) Exact ingredient Name match, 3) Contains match
    """
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    ingredients_db = user_data.get('notion_ingredients_db')
    recipe_items_db = user_data.get('notion_recipe_items_db')

    if not token or not ingredients_db or not recipe_items_db:
        return jsonify({'error': 'Recipe system not fully set up'}), 400

    data = request.json or {}
    recipe_id = data.get('recipe_id')
    items = data.get('items', [])

    if not recipe_id:
        return jsonify({'error': 'recipe_id is required'}), 400
    if not items:
        return jsonify({'error': 'items list is required'}), 400

    # Build full ingredient index: canonical names AND raw names
    canonical_cache = {}  # canonical_name -> {page_id, last_updated, recipe_unit}
    name_cache = {}       # raw ingredient name (uppercase) -> {page_id, recipe_unit}

    has_more = True
    start_cursor = None
    while has_more:
        body = {'page_size': 100}
        if start_cursor:
            body['start_cursor'] = start_cursor
        resp = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
            json=body, headers=_notion_headers(token))
        if resp.status_code != 200:
            return jsonify({'error': f'Failed to query ingredients: {resp.text}'}), 502
        qdata = resp.json()
        for page in qdata.get('results', []):
            page_id = page['id']

            # Get Recipe Unit from ingredient
            ru_prop = page.get('properties', {}).get('Recipe Unit', {}).get('select', {})
            recipe_unit = ru_prop.get('name', 'oz') if ru_prop else 'oz'

            # Index by raw name
            title = page.get('properties', {}).get('Name', {}).get('title', [])
            raw_name = ''.join(t.get('plain_text', '') for t in title).strip() if title else ''
            if raw_name:
                name_cache[raw_name.upper()] = {'page_id': page_id, 'recipe_unit': recipe_unit}

            # Index by canonical name (keep most recently updated)
            cn = page.get('properties', {}).get('Canonical Name', {}).get('select', {})
            if cn and cn.get('name'):
                cname = cn['name']
                last_updated = ''
                lu_prop = page.get('properties', {}).get('Last Updated', {}).get('date')
                if lu_prop:
                    last_updated = lu_prop.get('start', '')
                if cname not in canonical_cache or last_updated > canonical_cache[cname]['last_updated']:
                    canonical_cache[cname] = {'page_id': page_id, 'last_updated': last_updated, 'recipe_unit': recipe_unit}
        has_more = qdata.get('has_more', False)
        start_cursor = qdata.get('next_cursor')

    created = 0
    errors_list = []

    for item in items:
        item_name = item.get('name', '') or item.get('canonical_name', '')
        qty = item.get('qty', 0)
        unit_override = item.get('unit')  # None if not specified — will use ingredient's Recipe Unit
        ingredient_id = None
        recipe_unit = 'oz'  # default
        display_name = item_name
        is_canonical = False

        # 1) Try canonical name match first
        if item_name in canonical_cache:
            ingredient_id = canonical_cache[item_name]['page_id']
            recipe_unit = canonical_cache[item_name]['recipe_unit']
            is_canonical = True
        # 2) Try exact raw name match (case-insensitive)
        elif item_name.upper() in name_cache:
            match = name_cache[item_name.upper()]
            ingredient_id = match['page_id']
            recipe_unit = match['recipe_unit']
        # 3) Try partial match (contains, case-insensitive)
        else:
            search_upper = item_name.upper()
            for raw_name, match in name_cache.items():
                if search_upper in raw_name:
                    ingredient_id = match['page_id']
                    recipe_unit = match['recipe_unit']
                    display_name = raw_name  # use the full raw name
                    break

        if not ingredient_id:
            errors_list.append(f"No ingredient found for '{item_name}'")
            continue

        # Use ingredient's Recipe Unit unless explicitly overridden
        unit = unit_override or recipe_unit

        # Create the Recipe Item with all relations set
        props = {
            'Name': {'title': [{'text': {'content': display_name}}]},
            'Recipe': {'relation': [{'id': recipe_id}]},
            'Ingredient': {'relation': [{'id': ingredient_id}]},
            'Qty Used': {'number': qty},
            'Unit': {'select': {'name': unit}},
        }
        # Set Canonical Name if it was a canonical match
        if is_canonical:
            props['Canonical Name'] = {'select': {'name': item_name}}

        page_id = _notion_create_page(token, recipe_items_db, props)
        if page_id:
            created += 1
        else:
            errors_list.append(f"Failed to create recipe item for '{cn}'")

    return jsonify({
        'ok': True,
        'created': created,
        'errors': errors_list,
        'available_canonical_names': list(canonical_cache.keys())
    })


# ── Canonical Name Resolution (shared logic) ─────────────
def _resolve_canonical_names(token, ingredients_db, recipe_items_db):
    """For each Recipe Item with a Canonical Name, find the latest ingredient
    with that canonical name and set the Ingredient relation."""

    # Step 1: Build a cache of canonical name → best ingredient ID
    canonical_cache = {}  # canonical_name -> {page_id, last_updated}
    has_more = True
    start_cursor = None
    while has_more:
        body = {'page_size': 100, 'filter': {
            'property': 'Canonical Name',
            'select': {'is_not_empty': True}
        }}
        if start_cursor:
            body['start_cursor'] = start_cursor
        resp = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
            json=body, headers=_notion_headers(token))
        if resp.status_code != 200:
            logging.error(f"Canonical resolve: failed to query ingredients: {resp.text}")
            return {'resolved': 0, 'error': 'Failed to query ingredients'}
        data = resp.json()
        for page in data.get('results', []):
            cn = page.get('properties', {}).get('Canonical Name', {}).get('select', {})
            if not cn:
                continue
            name = cn.get('name', '')
            last_updated = ''
            lu_prop = page.get('properties', {}).get('Last Updated', {}).get('date')
            if lu_prop:
                last_updated = lu_prop.get('start', '')
            if name not in canonical_cache or last_updated > canonical_cache[name]['last_updated']:
                canonical_cache[name] = {'page_id': page['id'], 'last_updated': last_updated}
        has_more = data.get('has_more', False)
        start_cursor = data.get('next_cursor')

    if not canonical_cache:
        return {'resolved': 0, 'message': 'No ingredients have Canonical Names set yet'}

    # Step 2: Query all recipe items that have a Canonical Name set
    resolved = 0
    skipped = 0
    has_more = True
    start_cursor = None
    while has_more:
        body = {'page_size': 100, 'filter': {
            'property': 'Canonical Name',
            'select': {'is_not_empty': True}
        }}
        if start_cursor:
            body['start_cursor'] = start_cursor
        resp = requests.post(f'https://api.notion.com/v1/databases/{recipe_items_db}/query',
            json=body, headers=_notion_headers(token))
        if resp.status_code != 200:
            logging.error(f"Canonical resolve: failed to query recipe items: {resp.text}")
            break
        data = resp.json()
        for page in data.get('results', []):
            cn = page.get('properties', {}).get('Canonical Name', {}).get('select', {})
            if not cn:
                continue
            name = cn.get('name', '')
            if name not in canonical_cache:
                skipped += 1
                continue

            best_ingredient_id = canonical_cache[name]['page_id']
            requests.patch(f'https://api.notion.com/v1/pages/{page["id"]}',
                json={'properties': {
                    'Ingredient': {'relation': [{'id': best_ingredient_id}]}
                }}, headers=_notion_headers(token))
            resolved += 1

        has_more = data.get('has_more', False)
        start_cursor = data.get('next_cursor')

    logging.info(f"Canonical resolve: resolved={resolved}, skipped={skipped}, canonical_names={list(canonical_cache.keys())}")
    return {'resolved': resolved, 'skipped': skipped, 'canonical_names': list(canonical_cache.keys())}


@app.route('/api/recipe/resolve-canonical', methods=['POST'])
@require_auth
def resolve_canonical():
    """API endpoint wrapper for canonical name resolution."""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    ingredients_db = user_data.get('notion_ingredients_db')
    recipe_items_db = user_data.get('notion_recipe_items_db')

    if not token or not ingredients_db or not recipe_items_db:
        return jsonify({'error': 'Recipe system not fully set up'}), 400

    result = _resolve_canonical_names(token, ingredients_db, recipe_items_db)
    result['ok'] = True
    return jsonify(result)


# ── Routes: Repair Recipe DB IDs ─────────────────────────
@app.route('/api/recipe/repair-ids', methods=['POST'])
@require_auth
def repair_recipe_ids():
    """Re-link recipe database IDs if they got lost"""
    user_data = load_user(request.user_id)
    data = request.json or {}
    ingredients_db = data.get('ingredients_db')
    recipes_db = data.get('recipes_db')
    recipe_items_db = data.get('recipe_items_db')
    if not ingredients_db or not recipes_db or not recipe_items_db:
        return jsonify({'error': 'Must provide ingredients_db, recipes_db, recipe_items_db'}), 400
    user_data['notion_ingredients_db'] = ingredients_db
    user_data['notion_recipes_db'] = recipes_db
    user_data['notion_recipe_items_db'] = recipe_items_db
    save_user(user_data)
    return jsonify({'ok': True, 'ingredients_db': ingredients_db, 'recipes_db': recipes_db, 'recipe_items_db': recipe_items_db})


# ── Routes: Sync Ingredients from Invoices ───────────────
@app.route('/api/recipe/sync-ingredients', methods=['POST'])
@require_auth
def sync_ingredients():
    """Back-fill Ingredients database from all existing invoice pages.
    Supports ?limit=N&offset=M to batch process and avoid gateway timeouts."""
    user_data = load_user(request.user_id)
    token = user_data.get('notion_token')
    invoices_db = user_data.get('notion_database_id')
    ingredients_db = user_data.get('notion_ingredients_db')

    if not token:
        return jsonify({'error': 'Notion not connected'}), 400
    if not invoices_db:
        return jsonify({'error': 'No invoices database set up'}), 400
    if not ingredients_db:
        return jsonify({'error': 'Recipe Costing not set up — run setup first'}), 400

    # Batch params to avoid gateway timeout
    req_data = request.json or {}
    batch_limit = req_data.get('limit', 5)  # process N invoices per call
    batch_offset = req_data.get('offset', 0)

    # Step 1: Query all invoice pages
    all_pages = []
    has_more = True
    start_cursor = None
    while has_more:
        body = {'page_size': 100}
        if start_cursor:
            body['start_cursor'] = start_cursor
        resp = requests.post(f'https://api.notion.com/v1/databases/{invoices_db}/query',
            json=body, headers=_notion_headers(token))
        if resp.status_code != 200:
            return jsonify({'error': f'Failed to query invoices: {resp.text}'}), 502
        data = resp.json()
        all_pages.extend(data.get('results', []))
        has_more = data.get('has_more', False)
        start_cursor = data.get('next_cursor')

    total_invoices = len(all_pages)
    # Slice to the batch window
    batch_pages = all_pages[batch_offset:batch_offset + batch_limit]

    created = 0
    updated = 0
    skipped = 0
    errors_list = []

    for page in batch_pages:
        page_id = page['id']
        # Get vendor name from page title
        vendor = ''
        title_prop = page.get('properties', {}).get('Vendor', {})
        if title_prop.get('title'):
            vendor = ''.join(t.get('plain_text', '') for t in title_prop['title'])

        inv_num = ''
        inv_prop = page.get('properties', {}).get('Invoice #', {})
        if inv_prop.get('rich_text'):
            inv_num = ''.join(t.get('plain_text', '') for t in inv_prop['rich_text'])

        # Step 2: Read page children to find the table block
        children_resp = requests.get(f'https://api.notion.com/v1/blocks/{page_id}/children?page_size=100',
            headers=_notion_headers(token))
        if children_resp.status_code != 200:
            errors_list.append(f"Could not read invoice {inv_num or page_id}")
            continue

        blocks = children_resp.json().get('results', [])

        # Find the table block with line items
        for block in blocks:
            if block.get('type') != 'table':
                continue

            # Read table rows (child blocks of the table)
            table_id = block['id']
            rows_resp = requests.get(f'https://api.notion.com/v1/blocks/{table_id}/children?page_size=100',
                headers=_notion_headers(token))
            if rows_resp.status_code != 200:
                continue

            rows = rows_resp.json().get('results', [])

            # Detect table format: 5 cols = new (Item, Pack/Size, Qty, UnitPrice, Total)
            #                      4 cols = old (Item, Qty, UnitPrice, Total)
            header_row = rows[0] if rows else None
            header_cells = header_row.get('table_row', {}).get('cells', []) if header_row else []
            has_pack_col = len(header_cells) >= 5

            # Skip header row (first row)
            for row in rows[1:]:
                cells = row.get('table_row', {}).get('cells', [])
                min_cols = 5 if has_pack_col else 4
                if len(cells) < min_cols:
                    continue

                if has_pack_col:
                    item_name = ''.join(t.get('plain_text', '') for t in cells[0]).strip()
                    pack_size_str = ''.join(t.get('plain_text', '') for t in cells[1]).strip()
                    qty_str = ''.join(t.get('plain_text', '') for t in cells[2]).strip()
                    unit_price_str = ''.join(t.get('plain_text', '') for t in cells[3]).strip().replace('$', '').replace(',', '')
                    total_str = ''.join(t.get('plain_text', '') for t in cells[4]).strip().replace('$', '').replace(',', '')
                else:
                    item_name = ''.join(t.get('plain_text', '') for t in cells[0]).strip()
                    pack_size_str = ''
                    qty_str = ''.join(t.get('plain_text', '') for t in cells[1]).strip()
                    unit_price_str = ''.join(t.get('plain_text', '') for t in cells[2]).strip().replace('$', '').replace(',', '')
                    total_str = ''.join(t.get('plain_text', '') for t in cells[3]).strip().replace('$', '').replace(',', '')

                if not item_name:
                    continue

                try:
                    total_price = float(total_str) if total_str else 0
                except ValueError:
                    total_price = 0

                try:
                    unit_price = float(unit_price_str) if unit_price_str else 0
                except ValueError:
                    unit_price = 0

                try:
                    qty = float(qty_str) if qty_str else 1
                except ValueError:
                    qty = 1

                # Pack Price = price of ONE case/pack, not the line total
                pack_price = unit_price if unit_price > 0 else total_price

                if total_price <= 0:
                    skipped += 1
                    continue

                # Step 3: Check if ingredient already exists
                search_resp = requests.post(f'https://api.notion.com/v1/databases/{ingredients_db}/query',
                    json={'filter': {'property': 'Name', 'title': {'equals': item_name}}},
                    headers=_notion_headers(token))

                if search_resp.status_code != 200:
                    skipped += 1
                    continue

                results = search_resp.json().get('results', [])

                if results:
                    # Update existing ingredient
                    ing_page_id = results[0]['id']
                    update_props = {
                        'Pack Price': {'number': pack_price},
                        'Vendor': {'rich_text': [{'text': {'content': vendor}}]},
                        'Last Invoice': {'rich_text': [{'text': {'content': inv_num}}]},
                        'Last Updated': {'date': {'start': datetime.now().strftime('%Y-%m-%d')}}
                    }
                    if pack_size_str:
                        update_props['Pack Size'] = {'rich_text': [{'text': {'content': pack_size_str}}]}
                        update_props['Pack Description'] = {'rich_text': [{'text': {'content': pack_size_str}}]}
                    requests.patch(f'https://api.notion.com/v1/pages/{ing_page_id}',
                        json={'properties': update_props}, headers=_notion_headers(token))
                    updated += 1
                else:
                    # Create new ingredient
                    pack_desc = pack_size_str or (f"{int(qty)} x ${unit_price:.2f}" if qty > 1 and unit_price > 0 else '')
                    create_props = {
                        'Name': {'title': [{'text': {'content': item_name}}]},
                        'Vendor': {'rich_text': [{'text': {'content': vendor}}]},
                        'Pack Price': {'number': pack_price},
                        'Pack Description': {'rich_text': [{'text': {'content': pack_desc}}]},
                        'Last Invoice': {'rich_text': [{'text': {'content': inv_num}}]},
                        'Last Updated': {'date': {'start': datetime.now().strftime('%Y-%m-%d')}}
                    }
                    if pack_size_str:
                        create_props['Pack Size'] = {'rich_text': [{'text': {'content': pack_size_str}}]}
                    _notion_create_page(token, ingredients_db, create_props)
                    created += 1

    next_offset = batch_offset + batch_limit
    has_more_batches = next_offset < total_invoices
    logging.info(f"Sync ingredients batch [offset={batch_offset}, limit={batch_limit}]: created={created}, updated={updated}, skipped={skipped}")
    return jsonify({
        'ok': True,
        'created': created,
        'updated': updated,
        'skipped': skipped,
        'invoices_scanned': len(batch_pages),
        'total_invoices': total_invoices,
        'next_offset': next_offset if has_more_batches else None,
        'has_more': has_more_batches,
        'errors': errors_list
    })


# ── Routes: Usage ────────────────────────────────────────
@app.route('/api/usage', methods=['GET'])
@require_auth
def get_usage():
    user_data = load_user(request.user_id)
    monthly = get_monthly_count(user_data)
    limit = get_scan_limit(user_data['tier'])
    return jsonify({
        'tier': user_data['tier'],
        'scans_used': monthly,
        'scans_limit': limit,
        'scans_remaining': max(0, limit - monthly),
        'total_scans': user_data['scan_count'],
        'notion_connected': bool(user_data.get('notion_token')),
        'recent_scans': user_data.get('scans', [])[-10:]
    })

@app.route('/api/export/csv', methods=['POST'])
@require_auth
def export_csv():
    """Generate CSV from invoice data"""
    invoice = request.json.get('invoice', {})
    items = invoice.get('items', [])
    lines = ['Item,Item Code,Pack Size,Qty,Unit Price,Total']
    for i in items:
        lines.append(f'"{i.get("item_name","")}","{i.get("item_code","")}","{i.get("pack_size","")}",{i.get("quantity",0)},{i.get("unit_price",0)},{i.get("total_price",0)}')
    lines.append(f',,,,Subtotal,{invoice.get("subtotal",0)}')
    if invoice.get('tax'):
        lines.append(f',,,,Tax,{invoice["tax"]}')
    lines.append(f',,,,Total,{invoice.get("total",0)}')
    return '\n'.join(lines), 200, {'Content-Type': 'text/csv', 'Content-Disposition': f'attachment; filename=invoice-{invoice.get("invoice_number","scan")}.csv'}


# ── Helpers ──────────────────────────────────────────────
def _parse_date(date_str):
    """Convert MM/DD/YYYY to YYYY-MM-DD for Notion"""
    if not date_str:
        return datetime.now().strftime('%Y-%m-%d')
    try:
        for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%m-%d-%Y']:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return date_str
    except Exception:
        return datetime.now().strftime('%Y-%m-%d')


# ── Run ──────────────────────────────────────────────────
if __name__ == '__main__':
    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY not set!")
    app.run(host='0.0.0.0', port=8081, debug=True)
