from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class ArxivWeblink:
    href: str
    title: str


@dataclass
class ArxivCategories:
    primary_category: str
    categories: [str]


@dataclass
class ArxivAuthor:
    name: str
    affiliations: List[str]


@dataclass
class ArxivEntry:
    id: str
    doi: str
    title: str
    published_date: str
    journal_ref: Optional[str]
    summary: Optional[str]
    comment: Optional[str]

    authors: List[ArxivAuthor]
    categories: ArxivCategories
    weblinks: List[ArxivWeblink]


class ArxivTransformObj:
    """
    The Goal is to create objects that match the source!
    Later we map these objects to ORM!
    """

    def extract(self, data: Dict[str, Any]) -> ArxivEntry:
        doc = data.get("ns0:entry", {})

        authors = self._extract_authors(doc)
        categories = self._extract_categories(doc)
        weblinks = self._extract_weblinks(doc)

        return ArxivEntry(
            id=doc.get("ns0:id"),
            doi=doc.get("ns0:doi"),
            title=doc.get("ns0:title"),
            published_date=doc.get("ns0:published"),
            journal_ref=doc.get("ns1:journal_ref"),
            summary=doc.get("ns0:summary"),
            comment=doc.get("ns1:comment"),
            authors=authors,
            categories=categories,
            weblinks=weblinks,
        )

    def _extract_authors(self, doc: dict):
        authors = []
        for author_data in self.ensure_list(doc.get("ns0:author", [])):
            affiliations = []
            for affiliation in self.ensure_list(author_data.get("ns1:affiliation", [])):
                affiliations.append(affiliation.get("text"))
            if name := author_data.get("ns0:name"):
                authors.append(ArxivAuthor(name=name, affiliations=affiliations))
        return authors

    def _extract_categories(self, doc: dict):
        categories = []
        for category in self.ensure_list(doc.get("ns0:category")):
            categories.append(category.get("@term"))
        return ArxivCategories(
            primary_category=doc.get("ns1:primary_category").get("@term"),
            categories=categories,
        )

    def _extract_weblinks(self, doc: dict):
        weblinks = []
        for weblink in self.ensure_list(doc.get("ns0:link")):
            weblinks.append(
                ArxivWeblink(href=weblink.get("@href"), title=weblink.get("@title"))
            )
        return weblinks

    @staticmethod
    def ensure_list(value) -> list:
        """
        Ensures the input value is a list. If it's not a list, wraps it in one.
        If the value is None, returns an empty list.
        """
        if value is None:
            return []
        return value if isinstance(value, list) else [value]
