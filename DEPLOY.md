# TicketSnap — Deploy to Beelink

## 1. Copy files to Beelink

```bash
# From your Mac — copy the entire ticketsnap folder
scp -P 2222 -r ticketsnap mike@ssh.rednun.com:/opt/
```

## 2. SSH in and set up

```bash
ssh -p 2222 mike@ssh.rednun.com

# Create venv (separate from Red Nun)
cd /opt/ticketsnap
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create env file with your API key
cat > .env << 'EOF'
ANTHROPIC_API_KEY=sk-ant-YOUR-KEY-HERE
TICKETSNAP_SECRET=CHANGE_ME_TO_RANDOM_STRING
EOF

# Quick test
source .env && export ANTHROPIC_API_KEY TICKETSNAP_SECRET
python app.py
# Should say: Running on http://0.0.0.0:8081
# Ctrl+C to stop
```

## 3. Create systemd service

```bash
sudo tee /etc/systemd/system/ticketsnap.service << 'EOF'
[Unit]
Description=TicketSnap Invoice Scanner
After=network.target

[Service]
User=mike
WorkingDirectory=/opt/ticketsnap
EnvironmentFile=/opt/ticketsnap/.env
ExecStart=/opt/ticketsnap/venv/bin/gunicorn -w 2 -b 127.0.0.1:8081 --timeout 120 app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ticketsnap
sudo systemctl start ticketsnap
sudo systemctl status ticketsnap
```

## 4. Add nginx route

```bash
# Edit your existing nginx config
sudo nano /etc/nginx/sites-available/rednun

# Add this ABOVE the existing location / block:

    # TicketSnap — separate app on port 8081
    location /scanner {
        rewrite ^/scanner$ / break;
        rewrite ^/scanner/(.*)$ /$1 break;
        proxy_pass http://127.0.0.1:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        client_max_body_size 20M;
    }

# Test and reload
sudo nginx -t
sudo systemctl reload nginx
```

## 5. Test on iPhone

Open Safari: `https://rednun.com/scanner`

1. Tap 📷 Camera
2. Snap a photo of an invoice
3. Hit "Scan Invoice"
4. Wait 5-10 seconds
5. See extracted data with math validation

## 6. Later: Move to ticketsnap.com

When ready to buy the domain:

1. Buy ticketsnap.com (Namecheap, Cloudflare, etc.)
2. Add A record pointing to your Comcast IP (same as rednun.com)
3. Add to ddns_update.sh so it stays in sync
4. Create a separate nginx server block for ticketsnap.com
5. Get a Let's Encrypt cert: `sudo certbot --nginx -d ticketsnap.com`

## File Structure

```
/opt/ticketsnap/
├── app.py              ← Flask backend (API key here, not in frontend)
├── requirements.txt
├── .env                ← ANTHROPIC_API_KEY + secret
├── venv/               ← Python virtual environment
├── data/               ← User data (auto-created)
│   └── demo_xxx.json   ← Per-user scan history
└── static/
    └── index.html      ← Mobile-first frontend
```

## Completely separate from Red Nun

- Different directory: /opt/ticketsnap (not /opt/rednun)
- Different venv
- Different systemd service
- Different port (8081 vs 8080)
- Shares nothing with the dashboard
```
