from typing import List, Callable, Dict

from pymongo import InsertOne
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.remote.webelement import WebElement


class RawArtist:

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url

    def __str__(self):
        return self.name


class RawReviewer:

    def __init__(self, name: str, type: str, url: str):
        self.name = name
        self.type = type
        self.url = url

    def __str__(self):
        return "{} {}".format(self.name, self.type)


class RawRelatedReview:

    def __init__(self, url: str, album_artwork_url: str, album_name: str, reviewers: List[str], date_time: str,
                 abstract: str):
        self.url = url
        self.album_artwork_url = album_artwork_url
        self.album_name = album_name
        self.reviewers = reviewers
        self.date_time = date_time
        self.abstract = abstract

    def __str__(self):
        return "{} {} {} {}".format(self.url, self.album_name, self.reviewers, self.date_time)


class RawReview:

    def __init__(self, url: str, article_id: str, artists: List[RawArtist], album_name: str, album_artwork_url,
                 labels: List[str],
                 year: str, rating: str, best_new: str, reviewers: List[RawReviewer], genres: List[str], date_time: str,
                 review_abstract: str, review_body: str, related_reviews: List[RawRelatedReview]):
        self.url = url
        self.article_id = article_id
        self.artists = artists
        self.album_name = album_name
        self.album_artwork_url = album_artwork_url
        self.labels = labels
        self.year = year
        self.rating = rating
        self.best_new = best_new
        self.reviewers = reviewers
        self.genres = genres
        self.date_time = date_time
        self.review_abstract = review_abstract
        self.review_body = review_body
        self.related_reviews = related_reviews

    def __str__(self):
        return "{} {} {} {} {} {} {}".format(self.url, self.artists, self.album_name, self.date_time, self.rating,
                                             self.genres, self.reviewers)

    def get_dict_model(self) -> Dict:
        return {
            "url": self.url,
            "article_id": self.article_id,
            "artists": list({
                                "url": artist.url,
                                "name": artist.name,
                            } for artist in self.artists),
            "album_name": self.album_name,
            "album_artwork_url": self.album_artwork_url,
            "labels": list({
                               "label": label
                           } for label in self.labels),
            "year": self.year,
            "rating": self.rating,
            "best_new": self.best_new,
            "reviewers": list({
                                  "url": reviewer.url,
                                  "name": reviewer.name,
                                  "type": reviewer.type
                              } for reviewer in self.reviewers),
            "genres": list({
                               "genre": genre
                           } for genre in self.genres),
            "date_time": self.date_time,
            "review_abstract": self.review_abstract,
            "review_body": self.review_body,
            "related_reviews": list({
                                        "url": related_review.url,
                                        "album_artwork_url": related_review.album_artwork_url,
                                        "album_name": related_review.album_name,
                                        "reviewers": list({
                                                              "reviewer": related_reviewers
                                                          } for related_reviewers in related_review.reviewers),
                                        "date_time": related_review.date_time,
                                        "abstract": related_review.abstract,
                                    } for related_review in self.related_reviews)
        }


def parse_review_web_element(url: str, root_web_element: WebElement) -> RawReview:
    article_id = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector("article").get_attribute("id"))
    artists = get_artists(root_web_element)
    album_name = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector(".single-album-tombstone__review-title").text)
    album_artwork_url = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector(".single-album-tombstone__art img").get_attribute("src"))
    labels = list(label_web_element.text for label_web_element in
                  root_web_element.find_elements_by_css_selector(".single-album-tombstone__meta-labels li"))
    year = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector(".single-album-tombstone__meta-year").text)
    rating = _safe_parse_element(lambda: root_web_element.find_element_by_css_selector(".score").text)
    best_new = _safe_parse_element(lambda: root_web_element.find_element_by_css_selector(".bnm-txt").text)
    reviewers = get_reviewers(root_web_element)
    genres = list(label_web_element.text for label_web_element in
                  root_web_element.find_elements_by_css_selector(".genre-list li a"))
    date_time = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector(".pub-date").get_attribute("datetime"))
    review_abstract = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector(".review-detail__abstract").get_property("innerHTML"))
    review_body = _safe_parse_element(
        lambda: root_web_element.find_element_by_css_selector(".review-detail__text .contents").get_property(
            "innerHTML"))
    related_reviews = get_related_reviews(root_web_element)
    return RawReview(url, article_id, artists, album_name, album_artwork_url, labels, year, rating, best_new, reviewers,
                     genres, date_time, review_abstract, review_body, related_reviews)


def get_artists(root_web_element: WebElement) -> List[RawArtist]:
    return list(RawArtist(artist_web_element.text, artist_web_element.get_attribute("href"))
                for artist_web_element in
                root_web_element.find_elements_by_css_selector(".single-album-tombstone__artist-links a"))


def get_reviewers(root_web_element: WebElement) -> List[RawReviewer]:
    return list(RawReviewer(_safe_parse_element(
        lambda: reviewer_web_element.find_element_by_css_selector(".authors-detail__display-name").text),
        _safe_parse_element(lambda: reviewer_web_element.find_element_by_css_selector(
            ".authors-detail__title").text),
        _safe_parse_element(lambda: reviewer_web_element.find_element_by_css_selector(
            ".authors-detail__display-name").get_attribute("href")))
                for reviewer_web_element in root_web_element.find_elements_by_css_selector(".authors-detail li"))


def get_related_reviews(root_web_element: WebElement) -> List[RawRelatedReview]:
    return list(RawRelatedReview(related_review_web_element.get_attribute("href"),
                                 _safe_parse_element(lambda: related_review_web_element.find_element_by_css_selector(
                                     ".review-related__albums__list-item-artwork img").get_attribute("src")),
                                 _safe_parse_element(lambda: related_review_web_element.find_element_by_css_selector(
                                     ".review-related__albums__list-item-title").text),
                                 list(
                                     related_review_reviewer_web_element.text for related_review_reviewer_web_element in
                                     related_review_web_element.find_elements_by_css_selector(".authors li span")),
                                 _safe_parse_element(lambda: related_review_web_element.find_element_by_css_selector(
                                     ".pub-date").get_attribute("datetime")),
                                 _safe_parse_element(lambda: related_review_web_element.find_element_by_css_selector(
                                     ".review-related__albums__list-item-abstract").get_property("innerHTML")),
                                 ) for related_review_web_element in
                root_web_element.find_elements_by_css_selector(".related-albums__list a"))


def _safe_parse_element(parse_element: Callable, exception_return_value: str = "NA") -> str:
    try:
        return parse_element()
    except NoSuchElementException:
        return exception_return_value
