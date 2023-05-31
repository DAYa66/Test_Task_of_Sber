import undetected_chromedriver as uc
from selenium.webdriver.remote.webdriver import By
import  pandas as pd
import time
from random import randint
from datetime import datetime
from functools import lru_cache

class Avito_Parser:
    def __init__(self,
                 key_word: str,  # ключевое слово поиска
                 version_chrome: int, # версия браузера Chrome
                 cnt=2 # для тестирования ограничил кол-во страниц парсинга двумя
                ):
        self.start_url = f"https://www.avito.ru/novosibirsk?q={key_word}"
        self.key_word = key_word
        self.version_chrome = version_chrome
        self.cnt = cnt
        self.driver = None
        self.urls = set()
        self.parse_df = pd.DataFrame(columns=['ann_number', 'ann_title', 'ann_price', 'ann_address',
                                            'ann_description', 'ann_date', 'ann_views', 'ann_url',
                                            'key_word', 'parse_date', 'update_date'])

    def _set_driver(self):
        self.driver = uc.Chrome(version_main = self.version_chrome)

    def _get_url(self, url: str):
           self.driver.get(url)
           print(self.driver.current_url)
           print(self.driver.title)

    def _click_paginator(self):
        """Кнопка далее"""
        while self.driver.find_element(By.XPATH,
                                       "//nav[@aria-label='Пагинация']//a[@aria-label='Следующая страница']") and self.cnt > 0:
            self._parse_page()
            self.driver.find_element(By.XPATH,
                                     "//nav[@aria-label='Пагинация']//a[@aria-label='Следующая страница']").click()
            self.cnt -= 1

    def _parse_page(self):
        """Парсит открытую страницу"""

        titles = self.driver.find_elements(By.CSS_SELECTOR, "[data-marker='item']")
                                       #By.XPATH, "//div[@data-marker='catalog-serp']//div[@data-marker='item']") не работает
        print(f" Len titles: {len(titles)}")
        for title in titles[:2]:  # для тестирования ограничил кол-во объявлений на странице парсинга двумя
            name = title.find_element(By.CSS_SELECTOR, "[itemprop='name']").text
                                       #By.XPATH, "//h3[@itemprop='name']").text не работает
            print(name)
            url = title.find_element(By.CSS_SELECTOR,  "[data-marker='item-title']").get_attribute("href")
                                       #By.XPATH, "//a[@data-marker='item-title']").get_attribute("href") не работает
            self.urls.add(url)

            print(url)
            time.sleep(randint(3,11))
    def _write_to_parse_df(self, data_dict: dict):
        current_df = pd.DataFrame(
            data_dict
        )
        print(current_df)
        self.parse_df = pd.concat([self.parse_df, current_df], ignore_index=True)
        print(self.parse_df)

    def _parse_announce(self):
        for ann_url in self.urls:
            driver = uc.Chrome(version_main = self.version_chrome)
            driver.get(ann_url)

            print(f"ann_url: {ann_url}")

            ann_number = driver.find_element(By.CSS_SELECTOR, "[data-marker='item-view/item-id']").text
            print(f"ann_number: {ann_number}")
            ann_title = driver.find_element(By.XPATH, "//span[@data-marker='item-view/title-info']").text
            print(f"ann_title: {ann_title}")
            ann_address = driver.find_element(By.CSS_SELECTOR, "[itemprop='address']").text
            print(f"ann_address: {ann_address}")
            ann_description = driver.find_element(By.CSS_SELECTOR, "[itemprop='description']").text
            print(f"ann_description: {ann_description}")
            ann_date = driver.find_element(By.CSS_SELECTOR, "[data-marker='item-view/item-date']").text
            print(f"ann_date: {ann_date}")
            ann_views = driver.find_element(By.CSS_SELECTOR, "[data-marker='item-view/total-views']").text
            print(f"ann_views: {ann_views}")
            parse_date = datetime.now()
            print(f"parse_date: {parse_date}")
            try:
                ann_price = driver.find_element(By.CSS_SELECTOR, "[data-marker='item-view/item-price']").\
                    get_attribute('content')
            except Exception as error:
                print(f"Ошибка: {error}")
                ann_price = 'Цена не определена' # TODO: отследить все варианты кодирования цены и настроить ее парсинг
            print(f"ann_price: {ann_price}")
            data_dict = {
                'ann_number': [ann_number],
                'ann_title': [ann_title],
                'ann_price': [ann_price],
                'ann_address': [ann_address],
                'ann_description': [ann_description],
                'ann_date': [ann_date],
                'ann_views': [ann_views],
                'ann_url': [ann_url],
                'key_word': [self.key_word],
                'parse_date': [parse_date],
                'update_date': [parse_date]
            }
            driver.quit()
            self._write_to_parse_df(data_dict)

    @lru_cache(maxsize=None)
    def parse(self):
        """Метод для вызова"""
        try:
            self._set_driver()
            self._get_url(self.start_url)
            self._click_paginator()
            self._parse_announce()
        except Exception as error:
            print(f"Ошибка: {error}")

        return self.parse_df
