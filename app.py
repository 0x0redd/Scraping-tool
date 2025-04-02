# This version extends your original script to fetch detailed information from each school page.
import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
import argparse
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

# ========= ARGUMENTS CLI =========
parser = argparse.ArgumentParser(description="Scrape and export to Google Sheet")
parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
args = parser.parse_args()

# ========= VARIABLES ENV =========
GOOGLE_SERVICE_ACCOUNT_EMAIL = "csc-fsm-forms@csc-fsm-forms.iam.gserviceaccount.com"
GOOGLE_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/j4cDQimXpvpr
pzgyV1dQUIwyJjzeMSyJgVFUgP990mAdpy/QI5VPqlkNylLYmv42VEoaCG6ktTw5
2qrAsyh3Yqo9gqWMI7eXa+Xmv3FfdokTz19sMzFW/8Bjvz3+TH+GutlONIag8m8h
mj0Xmpmwp94pBcHcDBXNN7jcdDXNNnE/vnyl57dGM891ZG4X/Mr3iK3iYrgP9h+V
9SETRRDm215q2/uVfOcDMFTbwNY6glw10goHGFDGVYsdEZqtJxoarn6nfYE4ICfo
CtLUu9rqL1x57RIJtBNjLSGiIt7g7jSy04sD2LG7cqGXPE9/90f+4KwXh31CBpcG
l56G4Y5jAgMBAAECggEAA447VmyXEtLfDp3Q2v5aQBhFwebC58zkhy4xhnznXZGe
FG7S4iYjYK66GVKKScoTJBjLiRgqh1wetg7c/WXHsSwKUx5mkZIBCXxBcJ6RgtD+
NesIWfI7Ff8zC24JvzRkS1t2RuTjlX9qfDS9lY2vu7gh8byKEKFRrypYRy17sGPW
7ickBILi9t4fz8XtLIU1f2I1poTsSSHBo52OUq/lgyjGuQ4ZoODV1nzvK9Il4ne5
FwHUcY32WtzLXtcgHoGBOt2VhukNdqAHC1AXMklhF4n0/72giKBckuw/kKIOQN8C
hh7U9brNVhbBkqJHxCsWVrr+87uzEhRm0bOoSq3bIQKBgQD1dbmC9h6WbyBMAWJ6
d3yxVlOgDn6O+2y+kk5YbtYpgCNr60pvH5oPCQJSNXNnANAyiUenFbRpoBBMDo6f
wbhJiVmx4EI79OdFAGQz+RAsX3CSuWed4pn3iIM9Xnt/HXFuJLKTBT6NTkrdYfmI
2clxWWT/wRHDa+GxWCV6w1ZxXwKBgQDHyU2RYINek7C9ayFd1VARLjkjj6G5EBXA
CSb72Ofky0CrrrkcD8przvH8sQ/PtyjbF8UNxTQ4WYTAxTe3yzJCyYN73JNM0CS0
vq2rK/zn3yAK9h/RreG8geovvz56LcF2cKlAvRXlYnkeLMe+Fwi9SmGw6F/nYwW/
o3LvpFGtfQKBgQCtW53Pvp/Pw2BeDcRNlkI8xSl+q3364dvlpFMs5erfmJ5rw9vo
K7uUztoS0alVoB9q8cXnXc7zopagAM/+SMUtOpJcrHRbABauhx4+DrO7gxRwq/1g
ZwAy8PkvyKEUH+lSzxxH9bY9+oOpY17nplk9ctOARg2TpYfaxtSBpaBRpQKBgH4+
Eam/VsO4h4Hp5Beq5wE0OB7kb8jFBTlnjx6tHTtRw/e4XHgC8mHVTsviBp+2mTZ4
eqgM6MaCqoQ1GtQkrgnN1Cu8Ycez7r3Xj14OWa0bLOBATjLssSuw3A53xj7mEs2I
uyqRUPi1lNsgaMcRPxB9o+VpsNBBnixOWB5dss7xAoGAYu5SsskBcw0JFuNb/2Cr
n7nDyxifVcZjA76mUXz5eo9B5Q57xWUt6eUZf6lj4W3+9zqhFZv7plwWWqNV7D0I
1u/pcJBoaHV8pEUo24tdo81p1WFvgyDuXzvt3ML9s35LyK8ZEEts7G2qgCYard2+
CnGc7my6cB7fsXHGPxe4B5s=
-----END PRIVATE KEY-----"""
GOOGLE_SPREADSHEET_ID = "1OHZuyIAbT7YxPEMaUdW01liosE6m48TwE3T3-C0fwrc"

# ========= SCRAPING FUNCTION =========
def scrape_school_page(url):
    try:
        res = requests.get(url, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')

        data = {
            "Title": soup.find("h1", class_="title_ecole_hero").text.strip() if soup.find("h1", class_="title_ecole_hero") else "",
            "Description": "\n".join(p.get_text(strip=True) for p in soup.select("#top-info p")),
            "Website": "", "Appel": "", "WhatsApp": "", "Facebook": "", "Instagram": "",
            "Info": "", "Accreditations": "", "Conditions d'accès": "",
            "Dates des concours": "", "Masters": "", "Licences": "", "Autres écoles": "",
            "Link": url
        }

        # Contact links
        contact_section = soup.find("div", class_="chiffres-ctas")
        if contact_section:
            for a in contact_section.find_all("a", href=True):
                label = a.find("span", class_="social_hide")
                if label:
                    data[label.get_text(strip=True)] = a['href']

        # Info section
        info_entries = []
        for block in soup.select("div.ast-container.space_card_information div.card_infromation"):
            label = block.find("p")
            value = block.find("strong")
            if label and value:
                info_entries.append(f"{label.text.strip()}: {value.text.strip()}")
        data["Info"] = "\n".join(info_entries)

        # Accreditations
        cards = soup.select("div.card_label h5.title_label a")
        data["Accreditations"] = ", ".join(c.text.strip() for c in cards)

        # Admission conditions
        data["Conditions d'accès"] = ", ".join([cond.text.strip() for cond in soup.select("div.card_condition_dacces strong")])

        # Dates des concours
        concours = soup.select_one("div.site-content.grey-bg div.ast-row.mt_4.flex_cards")
        if concours:
            data["Dates des concours"] = concours.get_text("\n", strip=True)

        # Masters
        masters = [f"{m.text.strip()} ({m['href']})" for m in soup.select("div.card_formation h3.title_formation a") if "master" in m['href']]
        data["Masters"] = "\n".join(masters)

        # Licences
        licences = [f"{l.text.strip()} ({l['href']})" for l in soup.select("div.card_formation h3.title_formation a") if "licence" in l['href']]
        data["Licences"] = "\n".join(licences)

        # Autres écoles
        other = [f"{o.text.strip()} ({o['href']})" for o in soup.select("div.card_ecole h3.title_card_ecole a")]
        data["Autres écoles"] = "\n".join(other)

        return data

    except Exception as e:
        if args.verbose:
            print(f"❌ Error scraping {url}: {e}")
        return {}

# ========= STEP 1: EXTRACT ALL LINKS =========
base_page_url = "https://www.dates-concours.ma/ecoles-universites-au-maroc/?pages="
all_links = []

for page_num in range(1, 43):
    if args.verbose:
        print(f"📄 Scraping page {page_num}...")
    try:
        res = requests.get(base_page_url + str(page_num), timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        section = soup.find("div", class_="section_list_cards_ecole")
        if section:
            for a_tag in section.find_all("a", href=True):
                if "page-numbers" in a_tag.get("class", []):
                    continue
                title = a_tag.get_text(strip=True)
                href = a_tag['href']
                if href and title:
                    all_links.append(href)
    except Exception as e:
        print(f"❌ Error on page {page_num}: {e}")
    sleep(1)

# ========= STEP 2: SCRAPE EACH SCHOOL PAGE =========
all_data = []
for idx, url in enumerate(set(all_links)):
    if args.verbose:
        print(f"🔎 Scraping data for {url} ({idx+1}/{len(all_links)})")
    entry = scrape_school_page(url)
    if entry:
        all_data.append(entry)
    sleep(1)

# ========= STEP 3: EXPORT TO GOOGLE SHEETS =========
credentials_dict = {
    "type": "service_account",
    "project_id": "dummy",
    "private_key_id": "dummy",
    "private_key": GOOGLE_PRIVATE_KEY.replace("\\n", "\n"),
    "client_email": GOOGLE_SERVICE_ACCOUNT_EMAIL,
    "client_id": "dummy",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GOOGLE_SERVICE_ACCOUNT_EMAIL.replace('@', '%40')}"
}

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(GOOGLE_SPREADSHEET_ID)
worksheet = sheet.sheet1
worksheet.clear()

if all_data:
    df = pd.DataFrame(all_data)
    worksheet.append_row(df.columns.tolist())
    for i in range(0, len(df), 100):
        worksheet.append_rows(df.iloc[i:i+100].values.tolist())
        sleep(1)

print("✅ Tous les résultats détaillés ont été exportés vers Google Sheets.")
