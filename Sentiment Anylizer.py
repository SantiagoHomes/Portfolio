from nltk.sentiment import SentimentIntensityAnalyzer
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import pdfreader
from pdfreader import PDFDocument, SimplePDFViewer
import matplotlib.pyplot as plt
from selenium.common.exceptions import ElementNotInteractableException, ElementNotVisibleException, ElementClickInterceptedException, StaleElementReferenceException
import time
import yfinance as yf
from selenium.webdriver.chrome.service import Service
import pandas as pd


def analyze_sentiment(text):
  sia = SentimentIntensityAnalyzer()
  return sia.polarity_scores(text)

def parse_pdf(file_path):
    try:
        with open(file_path, "rb") as file:
            doc = PDFDocument(file)
            viewer = SimplePDFViewer(file)
            text = ""
            num_pages = len(list(doc.pages()))
            for page_number in range(1, num_pages + 1):
                viewer.navigate(page_number)
                viewer.render()
                text += " ".join(viewer.canvas.strings)
        return text
    except Exception as e:
        print(f"Error reading PDF {file_path}: {e}")
        return ""  # Return empty string in case of an error

    return text

def safe_find_element(driver, by, value, retries=3, delay=10):
    last_exception = None
    for _ in range(retries):
        try:
            element = WebDriverWait(driver, delay).until(EC.presence_of_element_located((by, value)))
            
            if element.is_displayed():
                return element
            else:
                raise ElementNotVisibleException(f"Element found by {by} = {value} is not visible")
        
        except (TimeoutException, ElementNotInteractableException, ElementNotVisibleException) as e:
            last_exception = e
            time.sleep(delay)

    raise last_exception

def safe_click(driver, by, value, retries=3, delay=100):
    last_exception = Exception("Unknown error in safe_click")  # Initialize with a default exception
    for _ in range(retries):
        try:
            element = safe_find_element(driver, by, value, retries, delay)
            try:
                # Attempt to click the element
                element.click()
                return
            except (ElementNotInteractableException, ElementClickInterceptedException, StaleElementReferenceException):
                # If regular click fails or the element is stale, retry
                continue
        except (TimeoutException, ElementNotInteractableException, ElementClickInterceptedException, StaleElementReferenceException) as e:
            last_exception = e
            time.sleep(delay)

    # If all retries fail, raise the last exception
    raise last_exception
    
def get_sentiment_from_pdf(file_path):
    pdf_text = parse_pdf(file_path)
    if pdf_text:  # Check if the text is not empty or None
        return analyze_sentiment(pdf_text)
    else:
        print(f"No text found in PDF {file_path}")
        return {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}  # Return neutral sentiment in case of an error

def main():
    PATH = "C:\Program Files (x86)\chromedriver.exe"
    service = Service(PATH)
    driver = webdriver.Chrome(service=service)
    driver.maximize_window()

    DOW30 = ['American Express Co', 'Amgen Inc', 'Apple Inc', 'Boeing Co', 'Caterpillar Inc', 'Cisco Systems Inc', 'Cheveron Corp',
             'Goldman Sachs Group Inc', 'Home Depot Inc', 'Honeywell International Inc', 'International Business Machines Corp', 
             'Intel Corp', 'Johnson & Johnson', 'Coca-Cola Co', 'JPMorgan Chase & Co', 'McDonald’s Corp', '3M Co', 'Merck & Co Inc',
             'Microsoft Corp', 'Nike Inc', 'Procter & Gamble Co', 'Travelers Companies Inc', 'UnitedHealth Group Inc', 'Salesforce Inc',
             'Verizon Communications Inc', 'Visa Inc', 'Walgreens Boots Alliance Inc', 'Walmart Inc', 'Walt Disney Co', 'Dow Inc']
    
    company_to_ticker = {
    'American Express Co': 'AXP',
    'Amgen Inc': 'AMGN',
    'Apple Inc': 'AAPL',
    'Boeing Co': 'BA',
    'Caterpillar Inc': 'CAT',
    'Cisco Systems Inc': 'CSCO',
    'Cheveron Corp': 'CVX',
    'Goldman Sachs Group Inc': 'GS',
    'Home Depot Inc': 'HD',
    'Honeywell International Inc': 'HON',
    'International Business Machines Corp': 'IBM',
    'Intel Corp': 'INTC',
    'Johnson & Johnson': 'JNJ',
    'Coca-Cola Co': 'KO',
    'JPMorgan Chase & Co': 'JPM',
    'McDonald’s Corp': 'MCD',
    '3M Co': 'MMM',
    'Merck & Co Inc': 'MRK',
    'Microsoft Corp': 'MSFT',
    'Nike Inc': 'NKE',
    'Procter & Gamble Co': 'PG',
    'Travelers Companies Inc': 'TRV',
    'UnitedHealth Group Inc': 'UNH',
    'Salesforce Inc': 'CRM',
    'Verizon Communications Inc': 'VZ',
    'Visa Inc': 'V',
    'Walgreens Boots Alliance Inc': 'WBA',
    'Walmart Inc': 'WMT',
    'Walt Disney Co': 'DIS',
    'Dow Inc': 'DOW'
    }
    
    # Opens UCF Nexis page & Signs-in using personal UCF credentials
    driver.get('https://guides.ucf.edu/nexis')
    safe_click(driver, By.XPATH, '//*[@id="s-lg-content-2806747"]/a')
    safe_find_element(driver, By.XPATH, '//*[@id="login-panel-body"]')
    safe_find_element(driver, By.XPATH, '//*[@id="userNameInput"]').send_keys("sa542838")
    safe_find_element(driver, By.XPATH, '//*[@id="passwordInput"]').send_keys("Lazy1112!!")
    safe_click(driver, By.XPATH, '//*[@id="submitButton"]')

    # Will search company in order and download firts 10 files according to restrictions prespecified
    safe_find_element(driver, By.XPATH, '//*[@id="ybJgkgk"]/ln-gns-searchbox/lng-searchbox/div[1]/lng-search-input/div/div/lng-expanding-textarea')
    safe_find_element(driver, By.XPATH, '//*[@id="ybJgkgk"]/ln-gns-searchbox/lng-searchbox/div[1]/lng-search-input/div/div/lng-expanding-textarea').send_keys(DOW30[0])
    safe_click(driver, By.XPATH, '//*[@id="ybJgkgk"]/ln-gns-searchbox/lng-searchbox/div[1]/date-selector/lng-search-options-menu/div/button/span')
    safe_click(driver, By.XPATH, '//*[@id="ybJgkgk"]/ln-gns-searchbox/lng-searchbox/div[1]/date-selector/lng-search-options-menu/div[2]/div/ul/li[1]/button/span')
    safe_click(driver, By.XPATH, '//*[@id="ybJgkgk"]/ln-gns-searchbox/lng-searchbox/div[1]/lng-search-button/button')
    safe_find_element(driver, By.XPATH, '//*[@id="sidebar"]/div/div[2]')
    safe_click(driver, By.XPATH, '//*[@id="podfiltersbuttondatestr-news"]')
    safe_find_element(driver, By.XPATH, '//*[@id="refine"]/div[2]/div[4]')

    safe_click(driver, By.XPATH, '//*[@id="refine"]/div[2]/div[4]/div[1]/button')
    safe_click(driver, By.XPATH, '//*[@id="selectyeartimeline"]')
    safe_click(driver, By.XPATH, '//*[@id="datepicker"]/h3/div[2]/ol/li[49]/button')
    safe_click(driver, By.XPATH, '//*[@id="selectmonthtimeline"]')
    safe_click(driver, By.XPATH, '//*[@id="datepicker"]/h3/div[1]/ol/li[7]/button')
    safe_click(driver, By.XPATH, '//*[@id="datepicker"]/table/tbody/tr[1]/td[6]/button')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/div[2]/div[4]/div[2]/button')
    safe_click(driver, By.XPATH, '//*[@id="selectmonthtimeline"]')
    safe_click(driver, By.XPATH, '//*[@id="datepicker"]/h3/div[1]/ol/li[12]/button')
    safe_click(driver, By.XPATH, '//*[@id="selectyeartimeline"]')
    safe_click(driver, By.XPATH, '//*[@id="datepicker"]/h3/div[2]/ol/li[2]/button')
    safe_click(driver, By.XPATH, '//*[@id="datepicker"]/table/tbody/tr[6]/td[7]/button')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/div[2]/div[4]/button')
    time.sleep(10)
    safe_click(driver, By.XPATH, '//*[@id="podfiltersbuttonpublicationtype"]')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/ul[2]/li[3]/label')
    time.sleep(10)
    safe_click(driver, By.XPATH, '//*[@id="podfiltersbuttonlanguage"]')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/ul[8]/li[1]/label/span[1]')
    time.sleep(10)
    safe_click(driver, By.XPATH, '//*[@id="podfiltersbuttonen-geography-news"]')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/ul[5]/li[6]/button')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/ul[5]/li[7]/label/span')
    time.sleep(25)
    safe_click(driver, By.XPATH, '//*[@id="podfiltersbuttonen-subject"]')
    safe_click(driver, By.XPATH, '//*[@id="refine"]/ul[2]/li[1]/label/span')
    time.sleep(30)
    safe_click(driver, By.XPATH, '//*[@id="sidebar"]/div/div[2]/menu/button[2]')
    time.sleep(5)
    safe_click(driver, By.XPATH, '/html/body/aside/form/footer/ul/li[2]/input')
    time.sleep(5)

    # Downloads 1st 10 files for 1st company
    safe_click(driver, By.XPATH, '//*[@id="results-list-toolbar-gvs"]/ul[1]/li[4]/ul/li[3]/button/span[1]')
    time.sleep(5)
    safe_find_element(driver, By.XPATH, '//*[@id="SelectedRange"]').send_keys("1-10")
    time.sleep(5)
    safe_click(driver, By.XPATH, '/html/body/aside/footer/div/button[1]')
    time.sleep(5)
    safe_click(driver, By.XPATH, '//*[@id="kwpyk"]/section/span[1]/button')
    time.sleep(5)
    safe_click(driver, By.XPATH, '//*[@id="recent-favorites"]/span[1]')
    time.sleep(5)
    safe_click(driver, By.XPATH, '//*[@id="recentfavoritesform"]/ul/li[1]/div/label/a')
    time.sleep(5)
    safe_click(driver, By.XPATH, '//*[@id="subSearch"]')
    time.sleep(5)
    
    def download_files(driver, company):
        safe_find_element(driver, By.XPATH, '//*[@id="searchTerms"]').clear()
        safe_find_element(driver, By.XPATH, '//*[@id="searchTerms"]').send_keys(company)
        safe_find_element(driver, By.XPATH, '//*[@id="searchTerms"]').send_keys(Keys.RETURN)
        safe_click(driver, By.XPATH, '//*[@id="kwpyk"]/section/span[1]/button')
        time.sleep(5)
        safe_click(driver, By.XPATH, '//*[@id="recent-favorites"]/span[1]')
        time.sleep(5)
        safe_click(driver, By.XPATH, '//*[@id="recentfavoritesform"]/ul/li[2]/div/label/a')
        time.sleep(5)
        safe_click(driver, By.XPATH, '//*[@id="subSearch"]')
        time.sleep(5)
        safe_click(driver, By.XPATH, '//*[@id="results-list-toolbar-gvs"]/ul[1]/li[4]/ul/li[3]/button')
        safe_find_element(driver, By.XPATH, '//*[@id="SelectedRange"]').send_keys("1-10")
        safe_click(driver, By.XPATH, '/html/body/aside/footer/div/button[1]')
           
    for i in range(len(DOW30)-1):
        time.sleep(5)
        download_files(driver, DOW30[i+1])
        time.sleep(25)

    sentiment_scores = {}
    for i, company in enumerate(DOW30):
        if i == 0:
            file_path = f"C:\\Users\\Santiago\\Downloads\\Files (10).PDF"
        else:
            file_path = f"C:\\Users\\Santiago\\Downloads\\Files (10) ({i}).PDF"
    
        sentiment_scores[company] = get_sentiment_from_pdf(file_path)


    sorted_data = sorted(sentiment_scores.items(), key=lambda x: x[1]['pos'], reverse=True)
    top_five_companies = sorted_data[:5]
    
    for item, inner_dict in top_five_companies:
        print(f"{item}: {inner_dict['pos']}")
        
    def fetch_data(ticker, start_date, end_date):
        stock_data = yf.download(ticker, start=start_date, end=end_date)
        return stock_data['Adj Close']
    
    def compute_returns(prices):
        return prices.pct_change().dropna()
    
    def plot_cumulative_returns(returns, title="Cumulative Returns"):
        (1 + returns).cumprod().plot(figsize=(10, 7), title=title)
        plt.xlabel("Date")
        plt.ylabel("Cumulative Returns")
        plt.show()

    def plot_sentiment_distribution(sentiment_scores):
        # Extract companies and scores
        companies = list(sentiment_scores.keys())
        pos_scores = [val['pos'] for val in sentiment_scores.values()]
    
        # Plotting
        plt.figure(figsize=(12, 8))
        plt.barh(companies, pos_scores, color='skyblue')
        plt.xlabel('Positive Sentiment Score')
        plt.ylabel('Company')
        plt.title('Positive Sentiment Distribution among DOW30 Companies')
        plt.show()

    def plot_top5_pie(sentiment_scores):
        # Sort sentiment_scores by positive sentiment and get top 5
        sorted_scores = sorted(sentiment_scores.items(), key=lambda x: x[1]['pos'], reverse=True)[:5]
    
        # Data for plotting
        labels = [item[0] for item in sorted_scores]
        sizes = [item[1]['pos'] for item in sorted_scores]
    
       # Plotting
        plt.figure(figsize=(10, 7))
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
        plt.title('Top 5 Companies by Positive Sentiment')
        plt.show()

    def plot_investment_allocation(sentiment_scores, investment=1000000):
        # Sort sentiment_scores by positive sentiment and get top 5
        sorted_scores = sorted(sentiment_scores.items(), key=lambda x: x[1]['pos'], reverse=True)[:5]
    
        # Data for plotting
        labels = [item[0] for item in sorted_scores]
        num_companies = len(labels)
        allocations = [investment / num_companies for _ in range(num_companies)]  # Splitting equally among top companies
    
        # Plotting
        plt.figure(figsize=(12, 8))
        plt.bar(labels, allocations, color='green')
        plt.xlabel('Company')
        plt.ylabel('Investment Amount ($)')
        plt.title('Investment Allocation for Top Companies')
        plt.show()
        
    def plot_combined_cumulative_returns(top5_returns, dow_returns, title="Cumulative Returns Comparison"):
        combined_df = pd.DataFrame({"Top 5 Companies": top5_returns, "DOW Index": dow_returns})
        combined_df.add(1).cumprod().plot(figsize=(10, 7), title=title)
        plt.xlabel("Date")
        plt.ylabel("Cumulative Returns")
        plt.legend()
        plt.grid(True)
        plt.show()
        
    start_date = '2022-07-01'  # specify the start date
    end_date = '2022-12-31'  # specify the end date
    
    top5_data = [fetch_data(company_to_ticker[ticker[0]], start_date, end_date) for ticker in top_five_companies]
    
    # Fetch data
    dow_data = fetch_data('^DJI', start_date, end_date)

    # Compute returns
    dow_returns = compute_returns(dow_data)
    top5_returns = [compute_returns(data) for data in top5_data]

    # Plot
    plot_cumulative_returns(dow_returns, title="DOW Index Cumulative Returns")
    for i, returns in enumerate(top5_returns):
        plot_cumulative_returns(returns, title=f"{top_five_companies[i][0]} Cumulative Returns")
        
    top5_combined_returns = sum(compute_returns(data) for data in top5_data) / 5
    # Call the functions
    plot_sentiment_distribution(sentiment_scores)
    plot_top5_pie(sentiment_scores)
    plot_investment_allocation(sentiment_scores)
    plot_combined_cumulative_returns(top5_combined_returns, dow_returns)

if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    