# Test_Task_of_Sber
Хитрое тестовое задание от Сбера.

   Стэк: selenium, undetected_chromedriver, sqlite3

   Создал MVP по парсингу объявлений на Авито.

   Основная сложность проекта состоит в том, что Авито в 2023-м году перестало парситься основными библиотеками, 
   
предназначенными для парсинга.

Единственный способ, который я нашел, это парсить Авито библиотекой undetected_chromedriver, под капотом которой 

находится selenium.

selenium разработан для тестирования сайтов, и для парсинга обычно применяется как вспомогательная библиотека. Но в 

   этом проекте его пришлось применять в качестве основной библиотеки. На данном этапе у проекта мало функций и есть 
   
не проработанные
   второстепенные моменты. На разработку проекта давалась неделя. И в связи с возникшими 

вышеописанными трудностями, львиная доля времени ушла на разработку дизайна проекта.