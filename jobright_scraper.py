import time
import csv
import json
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
import logging
import os

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#########################################
# 1. Database Functions
#########################################
def connect_db():
    try:
        conn = psycopg2.connect(
            host="jobscraping.c5wcoosa6ub8.eu-north-1.rds.amazonaws.com",
            database="job_scraping",
            user="postgres",
            password="L=HfA7=_S&nh8kQ",
            port=5432
        )
        logging.info("Connected to the database")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

def parse_csv_filename_to_category(csv_file):
    """Parse CSV filename to extract job category and determine level"""
    # Remove .csv extension and convert to lowercase
    base_name = os.path.splitext(csv_file)[0].lower()
    
    # Replace spaces and special characters with underscores
    category = base_name.replace(' ', '_').replace('&', 'and')
    
    # Map common variations
    category_mapping = {
        'accounting_and_finance': 'accounting_and_finance',
        'arts_and_entertainment': 'arts_and_entertainment', 
        'business_analyst': 'business_analyst',
        'consulting': 'consulting',
        'creatives_and_design': 'creatives_and_design',
        'data_analyst': 'data_analyst',
        'data_engineer': 'data_engineer',
        'engineering_and_development': 'engineering_and_development',
        'human_resources': 'human_resources',
        'legal_and_compliance': 'legal_and_compliance',
        'management_and_executive': 'management_and_executive',
        'marketing': 'marketing',
        'product_management': 'product_management',
        'project_manager': 'project_manager',
        'public_sector_and_government': 'public_sector_and_government',
        'sales': 'sales',
        'software_engineering': 'software_engineering',
        'customer_service_and_support': 'customer_service_and_support',
        'machine_learning_and_ai': 'machine_learning_and_ai',
        'education_and_training': 'education_and_training'
    }
    
    return category_mapping.get(category, category)

def ensure_unified_table_exists(conn):
    """Ensure the unified_jobs table exists (should already exist from migration)"""
    check_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'unified_jobs'
        )
    """
    try:
        with conn.cursor() as cur:
            cur.execute(check_sql)
            exists = cur.fetchone()[0]
            if exists:
                logging.info("Using existing unified_jobs table")
                return True
            else:
                logging.error("unified_jobs table not found! Please run the migration script first.")
                return False
    except Exception as e:
        logging.error(f"Error checking unified_jobs table: {e}")
        return False

def insert_into_unified_table(conn, job_data, job_category, job_level='newgrad', job_region='us'):
    """Insert job data into the unified_jobs table"""
    sql = """
        INSERT INTO unified_jobs (
            original_job_id, source_table, job_category, job_level, job_region,
            job_title, job_seniority, job_location, work_model, employment_type, job_summary,
            original_url, apply_link, salary_desc, publish_time, intern_hire_date, intern_graduate_date,
            core_responsibilities, skill_summaries, education_summaries, qualifications,
            preferred_have, jd_core_skills, company_id, company_name, company_size, company_desc,
            company_categories, company_found_year, company_location, company_url,
            fundraising_current_stage, created_at
        )
        VALUES (
            %(jobId)s, %(sourceTable)s, %(jobCategory)s, %(jobLevel)s, %(jobRegion)s,
            %(jobTitle)s, %(jobSeniority)s, %(jobLocation)s, %(workModel)s, %(employmentType)s, %(jobSummary)s,
            %(originalUrl)s, %(applyLink)s, %(salaryDesc)s, 
            CASE 
                WHEN %(publishTime)s ~ '^\\d{4}-\\d{2}-\\d{2}' THEN %(publishTime)s::timestamp
                ELSE NULL 
            END,
            CASE 
                WHEN %(internHireDate)s IS NOT NULL AND %(internHireDate)s ~ '^\\d{4}-\\d{2}-\\d{2}' THEN %(internHireDate)s::date
                ELSE NULL 
            END,
            %(internGraduateDate)s,
            %(coreResponsibilities)s::jsonb, %(skillSummaries)s::jsonb, %(educationSummaries)s::jsonb, %(qualifications)s::jsonb,
            %(preferredHave)s::jsonb, %(jdCoreSkills)s::jsonb, %(companyId)s, %(companyName)s, %(companySize)s, %(companyDesc)s,
            %(companyCategories)s, 
            CASE 
                WHEN %(companyFoundYear)s IS NOT NULL AND %(companyFoundYear)s ~ '^\\d+$' THEN %(companyFoundYear)s::integer
                ELSE NULL 
            END,
            %(companyLocation)s, %(companyURL)s, %(fundraisingCurrentStage)s, CURRENT_TIMESTAMP
        )
    """
    
    # Prepare data with additional metadata
    unified_data = dict(job_data)
    
    # Clean up date fields that might have non-date text
    if unified_data.get('internHireDate') and not unified_data['internHireDate'].replace('-', '').replace(' ', '').isdigit():
        if 'Start in' in unified_data['internHireDate']:
            unified_data['internHireDate'] = None
    
    unified_data.update({
        'sourceTable': f"web_scraper_{job_category}_{job_level}_{job_region}",
        'jobCategory': job_category,
        'jobLevel': job_level,
        'jobRegion': job_region
    })
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, unified_data)
        conn.commit()
        logging.info(f"Data inserted into unified_jobs table for category '{job_category}'")
        return True
    except Exception as e:
        logging.error(f"Error inserting data into unified_jobs: {e}")
        conn.rollback()
        return False

#########################################
# 2. Scraping Functions
#########################################
def login_to_jobright(driver, username, password):
    login_url = "https://jobright.ai/?login=true"
    driver.get(login_url)
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "form#basic"))
    )

    login_form = driver.find_element(By.CSS_SELECTOR, "form#basic")
    email_input = login_form.find_element(By.ID, "basic_email")
    password_input = login_form.find_element(By.ID, "basic_password")
    email_input.send_keys(username)
    password_input.send_keys(password)

    submit_button = login_form.find_element(By.CSS_SELECTOR, "button[type='submit']")
    submit_button.click()

    time.sleep(5)
    logging.info("Logged in successfully (assuming no errors)")

def scrape_job_data(driver, url):
    driver.get(url)
    time.sleep(10)

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    json_script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if json_script_tag and json_script_tag.string:
        data = json.loads(json_script_tag.string)
        data_source = data.get("props", {}).get("pageProps", {}).get("dataSource", {})
        job_result = data_source.get("jobResult", {})
        company_result = data_source.get("companyResult", {})

        job_data = {
            "jobId": job_result.get("jobId"),
            "jobTitle": job_result.get("jobTitle"),
            "jobSeniority": job_result.get("jobSeniority"),
            "jobLocation": job_result.get("jobLocation"),
            "workModel": job_result.get("workModel"),
            "publishTime": job_result.get("publishTime"),
            "employmentType": job_result.get("employmentType"),
            "jobSummary": job_result.get("jobSummary"),
            "originalUrl": job_result.get("originalUrl"),
            "applyLink": job_result.get("applyLink"),
            "salaryDesc": job_result.get("salaryDesc"),
            "coreResponsibilities": json.dumps(job_result.get("coreResponsibilities", [])),
            "skillSummaries": json.dumps(job_result.get("skillSummaries", [])),
            "educationSummaries": json.dumps(job_result.get("educationSummaries", [])),
            "internHireDate": job_result.get("internHireDate"),
            "internGraduateDate": job_result.get("internGraduateDate"),
            "qualifications": json.dumps(job_result.get("qualifications", {})),
            "preferredHave": json.dumps(job_result.get("preferredHave", {})),
            "jdCoreSkills": json.dumps(job_result.get("jdCoreSkills", [])),
            "companyId": company_result.get("companyId"),
            "companyName": company_result.get("companyName"),
            "companySize": company_result.get("companySize"),
            "companyDesc": company_result.get("companyDesc"),
            "companyCategories": company_result.get("companyCategories"),
            "companyFoundYear": company_result.get("companyFoundYear"),
            "companyLocation": company_result.get("companyLocation"),
            "companyURL": company_result.get("companyURL"),
            "fundraisingCurrentStage": company_result.get("fundraisingCurrentStage")
        }
        return job_data
    else:
        logging.warning(f"__NEXT_DATA__ script not found for URL {url}")
        return None

#########################################
# 3. Main Script
#########################################
def main():
    # List of CSV files
    csv_files = [
        "Data_Engineer.csv", "Management_and_Executive.csv", "Project_Manager.csv", "Software_Engineering.csv", 
        "Business_Analyst.csv", "Accounting_and_Finance.csv", "Machine_Learning_and_AI.csv", "Consulting.csv", 
        "Product_Management.csv", "Arts_and_Entertainment.csv", "Legal_and_Compliance.csv", "Marketing.csv", 
        "Public_Sector_and_Government.csv", "Data_Analyst.csv", "Creatives_and_Design.csv", "Human_Resources.csv", 
        "Education_and_Training.csv"
    ]
    
    # Get user inputs
    username = "artofai786@gmail.com"
    password = "karanjot786"
    
    # Job level - change this based on what you're scraping
    job_level = "newgrad"  # or "intern"

    conn = connect_db()

    # Check if unified_jobs table exists
    if not ensure_unified_table_exists(conn):
        logging.error("Cannot proceed without unified_jobs table. Exiting.")
        conn.close()
        return

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(10)

    try:
        login_to_jobright(driver, username, password)

        # Get the directory of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        total_processed = 0
        total_inserted = 0

        for csv_file in csv_files:
            csv_path = os.path.join(script_dir, csv_file)
            if not os.path.exists(csv_path):
                logging.warning(f"CSV file not found: {csv_path}")
                continue

            # Parse job category from filename
            job_category = parse_csv_filename_to_category(csv_file)
            logging.info(f"Processing {csv_file} -> Category: {job_category}, Level: {job_level}")

            with open(csv_path, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                urls = [row[0].strip() for row in reader if row]

            file_processed = 0
            file_inserted = 0

            for url in urls:
                try:
                    job_data = scrape_job_data(driver, url)
                    if job_data:
                        success = insert_into_unified_table(conn, job_data, job_category, job_level)
                        if success:
                            file_inserted += 1
                        file_processed += 1
                    else:
                        logging.warning(f"No data scraped for URL: {url}")
                        
                    # Add small delay to avoid being blocked
                    time.sleep(1)
                        
                except Exception as e:
                    logging.error(f"Error processing {url}: {str(e)}")

            total_processed += file_processed
            total_inserted += file_inserted
            
            logging.info(f"Finished processing {csv_file}: {file_inserted}/{file_processed} jobs inserted")

        logging.info(f"Migration completed: {total_inserted}/{total_processed} total jobs inserted into unified_jobs table")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        driver.quit()
        conn.close()
        logging.info("Closed the browser and DB connection")

if __name__ == "__main__":
    main()
