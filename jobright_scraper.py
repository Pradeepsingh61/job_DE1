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

def create_table_if_not_exists(conn, table_name):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        job_id TEXT UNIQUE,
        job_title TEXT,
        job_seniority TEXT,
        job_location TEXT,
        work_model TEXT,
        publish_time TEXT,
        employment_type TEXT,
        job_summary TEXT,
        original_url TEXT,
        apply_link TEXT,
        salary_Desc TEXT,
        core_responsibilities JSONB,
        skill_summaries JSONB,
        education_summaries JSONB,
        intern_hire_date TEXT,
        intern_graduate_date TEXT,
        qualifications JSONB,
        preferred_have JSONB,
        jd_core_skills JSONB,
        company_id TEXT,
        company_name TEXT,
        company_size TEXT,
        company_desc TEXT,
        company_categories TEXT,
        company_found_year TEXT,
        company_location TEXT,
        company_url TEXT,
        fundraising_current_stage TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        logging.info(f"Table '{table_name}' created or already exists")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()

def insert_into_postgres(conn, table_name, job_data):
    sql = f"""
        INSERT INTO {table_name} (
            job_id, job_title, job_seniority, job_location, work_model, publish_time,
            employment_type, job_summary, original_url, apply_link, salary_desc, core_responsibilities,
            skill_summaries, education_summaries, intern_hire_date, intern_graduate_date,
            qualifications, preferred_have, jd_core_skills, company_id, company_name,
            company_size, company_desc, company_categories, company_found_year, company_location,
            company_url, fundraising_current_stage
        )
        VALUES (
            %(jobId)s, %(jobTitle)s, %(jobSeniority)s, %(jobLocation)s, %(workModel)s, %(publishTime)s,
            %(employmentType)s, %(jobSummary)s, %(originalUrl)s, %(applyLink)s, %(salaryDesc)s, %(coreResponsibilities)s,
            %(skillSummaries)s, %(educationSummaries)s, %(internHireDate)s, %(internGraduateDate)s,
            %(qualifications)s, %(preferredHave)s, %(jdCoreSkills)s, %(companyId)s, %(companyName)s,
            %(companySize)s, %(companyDesc)s, %(companyCategories)s, %(companyFoundYear)s, %(companyLocation)s,
            %(companyURL)s, %(fundraisingCurrentStage)s
        )
        ON CONFLICT (job_id) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, job_data)
        conn.commit()
        logging.info(f"Data inserted into table '{table_name}'")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        conn.rollback()

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
        "Data_Engineer.csv", "Management_and_Executive.csv","Project_Manager.csv","Software_Engineering.csv","Business_Analyst.csv","Accounting_and_Finance.csv","Machine_Learning_and_AI.csv","Consulting.csv","Product_Management.csv","Arts_and_Entertainment.csv","Legal_and_Compliance.csv","Marketing.csv","Public_Sector_and_Government.csv","Data_Analyst.csv","Creatives_and_Design.csv","Human_Resources.csv","Education_and_Training.csv"
    ]
    # Get user inputs or use environment variables
    username = "artofai786@gmail.com"
    password = "karanjot786"

    conn = connect_db()

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Use ChromeDriverManager().install() directly
    # driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
    driver.implicitly_wait(10)

    try:
        login_to_jobright(driver, username, password)

        # Get the directory of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        for csv_file in csv_files:
            csv_path = os.path.join(script_dir, csv_file)
            if not os.path.exists(csv_path):
                logging.warning(f"CSV file not found: {csv_path}")
                continue

            # Update table_name generation as requested
            # table_name = os.path.splitext(csv_file)[0].lower() + "_intern_us"
            table_name = os.path.splitext(csv_file)[0].lower() + "_newgrad_us"
            create_table_if_not_exists(conn, table_name)

            with open(csv_path, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                urls = [row[0].strip() for row in reader if row]

            for url in urls:
                try:
                    job_data = scrape_job_data(driver, url)
                    if job_data:
                        insert_into_postgres(conn, table_name, job_data)
                    else:
                        logging.warning(f"No data scraped for URL: {url}")
                except Exception as e:
                    logging.error(f"Error processing {url}: {str(e)}")

            logging.info(f"Finished processing {csv_file}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        driver.quit()
        conn.close()
        logging.info("Closed the browser and DB connection")

if __name__ == "__main__":
    main()
