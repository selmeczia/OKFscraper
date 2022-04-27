from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException
import time
import pandas as pd
import logging
from datetime import datetime
import os


class OKFScraper:
    def __init__(self):

        self.col_order = ["Név", "Munkakör", "Kifizetőhely", "Nyilvántartási szám", "Érvényesség kezdete",
                          "Érvényesség vége", "Szakképesítés", "Státusz", "Korlátozott alkalmasság"]
        self.save_freq = 10
        self.sleep_slider = 0
        self.sleep_click = 0
        self.sleep_driver_init = 2

        self.logger_path = os.path.join(os.path.normpath(os.getcwd() + os.sep), 'logs')
        self.output_path = os.path.join(os.path.normpath(os.getcwd() + os.sep), 'data')

        self.chrome_driver_path = r"C:\Program Files (x86)\chromedriver.exe"

        self.url = "https://kereso.enkk.hu/index.php"

        self.input_data_path = "input_names_ext.xlsx"

        self.input_num_col = "num"
        self.input_name_col = "name"

        self.driver = None

        self.output_table = pd.DataFrame()
        self.all_rows = 0
        self.input_table = pd.DataFrame()
        self.results_len = 0
        self.current_index = 0

        self.run_start = datetime.now()
        self.timing_start = 0
        self.rolling_time = 0

        self.input_dict = {}

        self.actual_table = pd.DataFrame()

    def task_scheduler(self):
        self.init_browser()
        self.read_inputs()
        self.scrape_web()
        self.close_driver()
        self.save_file()

    def init_browser(self):
        chrome_driver_path = self.chrome_driver_path
        url = self.url
        sleep_driver_init = self.sleep_driver_init

        ser = Service(chrome_driver_path)
        op = webdriver.ChromeOptions()
        op.add_argument("--headless")
        op.add_argument("--disable-extensions")
        op.add_argument("--disable-dev-shm-usage")
        op.add_argument("--no-sandbox")
        self.driver = webdriver.Chrome(service=ser, options=op)
        self.driver.get(url)
        time.sleep(sleep_driver_init)

    def read_inputs(self):
        input_path = self.input_data_path
        input_num_col = self.input_num_col

        input_table = pd.read_excel(input_path)
        input_table[input_num_col] = input_table[input_num_col].astype(str)
        self.input_table = input_table
        self.all_rows = input_table.shape[0]

        self.timing_start = time.time()
        self.rolling_time = time.time()

    def scrape_web(self):

        input_table = self.input_table

        for index, row in input_table.iterrows():

            self.current_index = (index + 1)

            # Assign input values
            self.assign_inputs(row)

            self.logger(message=f'Scraping row {self.current_index} out of {self.all_rows}', print_details=False)

            # Writting search inputs
            self.search_inputs()

            # Slider
            self.action_slider()

            # Fill basic inputs
            person_basic_table = self.fill_basic_inputs()

            # Count search results
            self.count_search_res()

            # 1 result
            if self.results_len == 1:
                self.click_result()
                person_status_table = self.export_data()
                person_table = self.merge_person_data(person_basic_table, person_status_table)

            # 0 result
            elif self.results_len == 0:
                person_status_table = self.fill_status_error("NONAME")
                person_table = self.merge_person_data(person_basic_table, person_status_table)

            # Multiple result
            else:
                cur_search = self.input_dict["input_name"]
                cur_search = ' '.join(cur_search.replace("Dr.", "").rstrip().split(" ")[-2:])
                self.input_dict["search_name"] = cur_search
                # TODO: redo search from here - remove code duplication
                self.search_inputs()
                self.action_slider()

                self.count_search_res()

                if self.results_len == 1:
                    self.click_result()
                    person_status_table = self.export_data()
                    person_table = self.merge_person_data(person_basic_table, person_status_table)

                else:
                    person_status_table = self.fill_status_error("DUPLICATE")
                    person_table = self.merge_person_data(person_basic_table, person_status_table)

            person_table = person_table[self.col_order]
            self.output_table = pd.concat([self.output_table, person_table])

            if self.current_index % self.save_freq == 0:
                self.save_file()

            if self.current_index % 100 == 0:
                timing_end = time.time()
                time_elapsed = timing_end - self.rolling_time
                self.logger(f'Elapsed time for the last 100 rows: {time_elapsed} seconds', print_details=False)
                self.rolling_time = time.time()

    def close_driver(self):
        self.driver.close()

    def logger(self, message, print_details=True):
        logger_path = self.logger_path
        timestamp = self.run_start.strftime("%Y_%m_%d-%H-%M-%S")
        filename = f'log-{timestamp}.log'
        logger_path_file = os.path.join(logger_path, filename)

        logging.basicConfig(format="%(levelname)s - %(asctime)s : %(message)s",
                            filename=logger_path_file,
                            encoding='utf-8',
                            level=logging.INFO)
        if print_details:
            logger_message = \
                f'{message} number: {self.input_dict["search_num"]}, name: {self.input_dict["search_name"]}'
        else:
            logger_message = message
        logging.info(logger_message)
        #print(logger_message)

    def assign_inputs(self, row):
        input_dict = dict()
        input_dict["input_name"] = row["name"]
        input_dict["input_num"] = row["num"]
        input_dict["search_num"] = input_dict.get("input_num").split("-")[0]
        input_dict["search_name"] = \
            input_dict.get("input_name").replace("Dr.", "").replace("-", " ").rstrip().split(" ")[-1].lower()
        input_dict["input_job"] = row["Munkakör"]
        input_dict["input_pay"] = row["Kifizetőhely"]

        self.input_dict = input_dict

    def search_inputs(self):
        input_element = self.driver.find_element(By.ID, "nev")
        input_element.clear()
        input_element.send_keys(self.input_dict["search_name"])
        input_element = self.driver.find_element(By.ID, "szam")
        input_element.clear()
        input_element.send_keys(self.input_dict["search_num"])

    def action_slider(self):
        slider_element = self.driver.find_element(By.XPATH, '//*[@id="FORMkeres"]/div[2]/div[1]/div')
        if self.driver.find_element(By.XPATH, '//*[@id="FORMkeres"]/div[2]/div[3]').text.startswith("Zárva"):
            move = ActionChains(self.driver)
            move.click_and_hold(slider_element).move_by_offset(182, 0).release().perform()
            time.sleep(self.sleep_slider)
        self.driver.find_element(By.XPATH, '//*[@id="ok"]').click()
        time.sleep(self.sleep_click)

    def count_search_res(self):
        try:
            element_present = ec.presence_of_element_located((By.ID, "searchresultTABLE"))
            WebDriverWait(self.driver, 20).until(element_present)
        except TimeoutException:
            pass

        results = self.driver.find_elements(By.PARTIAL_LINK_TEXT, "Adatlap")
        self.results_len = len(results)

    def click_result(self):
        self.driver.find_element(By.XPATH, '//*[@id="searchresultTABLE"]/tbody/tr[4]/td[4]/a').click()

    def export_data(self):
        try:
            table = pd.read_html(self.driver.find_element(
                By.XPATH,
                '//*[@id="searchresultTABLE"]/tbody/tr[3]/td/table/tbody/tr[3]/td').get_attribute('outerHTML'))[0][1:]
            if table.shape[0] != 1:
                table.columns = table.iloc[0]
                table = table.drop(table.index[0])
                table = table.dropna(axis=1, how='all')
                table = table.drop_duplicates(subset=["Szakképesítés"], keep="first", ignore_index=True)
                table[["Érvényesség kezdete", "Érvényesség vége"]] = table["Érvényesség"].str.split(" - ", expand=True)
                person_status_table = table

            else:
                person_status_table = self.fill_status_error("EMPTY")
        except:
            person_status_table = self.fill_status_error("UNKNOWN")

        return person_status_table

    def fill_status_error(self, error):
        table = pd.DataFrame(columns=["Érvényesség kezdete", "Érvényesség vége", "Szakképesítés", "Státusz",
                                      "Korlátozott alkalmasság"])

        if error == "EMPTY":
            fill_msg = "# HIBA - ÜRES"
        elif error == "UNKNOWN":
            fill_msg = "# HIBA - ISMERETLEN HIBA"
        elif error == "NONAME":
            fill_msg = "# HIBA - HIÁNYZÓ NÉV"
        elif error == "DUPLICATE":
            fill_msg = "# HIBA - NÉV EGYEZŐSÉG"
        else:
            fill_msg = "# HIBA"

        table.loc[table.shape[0]] = fill_msg
        self.logger(message=f'Error at row {self.current_index} : {error}')

        return table

    def fill_basic_inputs(self):
        person_dict = dict()

        person_dict["Név"] = self.input_dict["input_name"]
        person_dict["Nyilvántartási szám"] = self.input_dict["search_num"]
        person_dict["Munkakör"] = self.input_dict["input_job"]
        person_dict["Kifizetőhely"] = self.input_dict["input_pay"]

        table = pd.DataFrame.from_dict(person_dict, orient='index').T
        return table

    def save_file(self):
        output_path = self.output_path
        timestamp = self.run_start.strftime("%Y_%m_%d-%H-%M-%S")
        filename = f'orvosi_adatbazis-{timestamp}.xlsx'
        save_path = os.path.join(output_path, filename)
        self.output_table.to_excel(save_path, index=False)

    @staticmethod
    def merge_person_data(person_basic_table, person_status_table):
        person_basic_table = \
            person_basic_table.loc[person_basic_table.index.repeat(person_status_table.shape[0])].reset_index(drop=True)
        person_table = pd.concat([person_basic_table, person_status_table], axis=1)

        return person_table


if __name__ == "__main__":
    scraper = OKFScraper()
    scraper.task_scheduler()
