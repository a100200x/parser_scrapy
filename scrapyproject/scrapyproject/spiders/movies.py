import datetime
import sys

import scrapy


class MoviesSpider(scrapy.Spider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_count = 0
        self.error_count = 0
    name = "movies"
    allowed_domains = ["ru.wikipedia.org"]
    custom_settings = {
        'FEEDS': {
            'movies.csv': {
                'format': 'csv',
                'encoding': 'utf-8-sig',
                'fields': ['name', 'genre', 'director', 'country', 'year',],
                'overwrite': True
            }
        },
        # 'LOG_LEVEL': 'DEBUG',
        'DOWNLOAD_DELAY': 0.1,
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    }

    def start_requests(self):
        url = "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"
        print("Парсер запущен. Можно остановить Ctrl+C . \n"
              "Список фильмов сохранится в файл .csv в папке spiders ")
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for movie in response.css('#mw-pages .mw-category-group ul li'):
            movie_link = movie.css('a::attr(href)').get()
            if movie_link:
                movie_url = response.urljoin(movie_link)
                yield scrapy.Request(
                    url=movie_url,
                    callback=self.parse_movie_details,
                )

        # Переход на следующую страницу категории
        next_page = response.css(
            '#mw-pages a:contains("Следующая страница")::attr(href)').get()
        if next_page:
            next_url = response.urljoin(next_page)
            yield response.follow(next_url, callback=self.parse)
        else:
            print(f"-------Завершено")
            self.logger.info("Last page")

    def parse_movie_details(self, response):
        """Парсинг детальной страницы фильма"""

        name = response.css('h1#firstHeading .mw-page-title-main::text').get()
        if not name:
            name = response.css('h1#firstHeading').xpath('string(.)').get()
        if not name:
            name = response.meta.get('movie_name', '')
        if name:
            import re
            name = re.sub(r'<[^>]+>', '', name)  # Убираем HTML теги
            name = re.sub(r'\s+', ' ', name).strip()  # Убираем лишние пробелы

        info_box = response.css('.infobox')

        # Жанр
        genre_raw = info_box.css('th:contains("Жанр") + td a::text').getall()
        if not genre_raw:
            genre_raw = info_box.css(
                'th:contains("Genre") + td a::text').getall()
        if not genre_raw:
            genre_raw = info_box.css(
                'th:contains("Жанры") + td a::text').getall()
        if not genre_raw:
            genre_text = info_box.css('th:contains("Жанр") + td::text').get()
            if not genre_text:
                genre_text = info_box.css(
                    'th:contains("Genre") + td::text').get()
            if not genre_text:
                genre_text = info_box.css(
                    'th:contains("Жанры") + td::text').get()
            if genre_text:
                # Разбиваем текст по запятым и другим разделителям
                import re
                genre_raw = [g.strip() for g in re.split(
                    r'[,;]', genre_text) if g.strip()]

        genre = []
        for g in genre_raw:
            g = g.strip()
            if (g and len(g) > 2 and not g.isdigit() and
                    not g.startswith('[') and g not in ['вд', 'и', 'and']):
                genre.append(g)
        if not genre:
            genre_span = info_box.css(
                'th:contains("Жанр") + td span::text').get()
            if genre_span:
                genre = [genre_span.strip()]

        # Режиссер
        director_raw = info_box.css(
            'th:contains("Режиссёр") + td a::text').getall()
        if not director_raw:
            director_text = info_box.css(
                'th:contains("Режиссёр") + td::text').get()
            if director_text and director_text.strip():
                director_raw = [director_text.strip()]
            else:
                director_text = info_box.css(
                    'th:contains("Режиссёр") + td span::text').get()
                if director_text and director_text.strip():
                    director_raw = [director_text.strip()]
                else:
                    director_text = info_box.css(
                        'th:contains("Режиссёр") + td span.no-wikidata::text').get()
                    if director_text and director_text.strip():
                        director_raw = [director_text.strip()]

        if not director_raw:
            director_text = info_box.css(
                'th:contains("Director") + td::text').get()
            if director_text and director_text.strip():
                director_raw = [director_text.strip()]
            else:
                director_text = info_box.css(
                    'th:contains("Director") + td span::text').get()
                if director_text and director_text.strip():
                    director_raw = [director_text.strip()]
                else:
                    director_text = info_box.css(
                        'th:contains("Director") + td span.no-wikidata::text').get()
                    if director_text and director_text.strip():
                        director_raw = [director_text.strip()]

        if not director_raw:
            director_raw = info_box.css(
                'th:contains("Director") + td a::text').getall()
        if not director_raw:
            director_raw = info_box.css(
                'th:contains("Режиссер") + td a::text').getall()

        director = []
        for d in director_raw:
            d = d.strip()
            if (d and len(d) > 2 and not d.isdigit() and
                    not d.startswith('[') and d not in ['вд', 'и', 'and']):
                director.append(d)
        director = ', '.join(director) if director else None

        # Страна
        country = None
        countries = []
        wraps = info_box.css('th:contains("Страны") + td .wrap::text').getall()
        if not wraps:
            links = info_box.css('th:contains("Страны") + td a::text').getall()
            if links:
                countries = links
            else:
                links = info_box.css(
                    'th:contains("Страна") + td a::text').getall()
                if links:
                    countries = links
        if wraps:
            countries = wraps
        else:
            wraps = info_box.css(
                'th:contains("Страна") + td .wrap::text').getall()
            if wraps:
                countries = wraps
        if countries:
            country = ', '.join(countries) if countries else None
        else:
            country_selectors = [
                'th:contains("Страна") + td .wrap::text',
                'th:contains("Страна") + td .country-name a::text',
                'th:contains("Страна") + td span[data-sort-value]::attr(data-sort-value)',
                'th:contains("Страна") + td a::text',
                'th:contains("Country") + td .wrap::text',
                'th:contains("Country") + td .country-name a::text',
                'th:contains("Country") + td a::text',
            ]

            for selector in country_selectors:
                country = info_box.css(selector).get()
                if country:
                    break

        # Год
        year_links = info_box.css(
            'th:contains("Год") + td .dtstart::text').getall()

        if not year_links:
            year_links = info_box.css(
                'th:contains("Год") + td a::text').getall()

        import re
        for link_text in year_links:
            # Ищем 4 цифры подряд (год)
            match = re.search(r'\b(19|20)\d{2}\b', link_text)
            if match:
                year = match.group()
                break

        def clean_text_list(text_list):
            return [text.strip() for text in text_list if text.strip()]

        def clean_text(text):
            return text.strip() if text else None

        sys.stdout.write(
            f"\rПарсим фильм: {name}                                      ")
        sys.stdout.flush()

        yield {
            'name': name if name else "Moovie_Name",
            'url': response.url,
            'genre': ', '.join(clean_text_list(genre)) if genre else "Genre",
            'director': director if director else 'Director',
            'country': country if country else "Country",
            'year': clean_text(year) if year else "YEAR",
            'category_page': response.meta.get('category_page', ''),
            'scraped_at': datetime.datetime.now().isoformat()
        }
