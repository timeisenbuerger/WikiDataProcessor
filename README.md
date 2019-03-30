# Repository: WikiDataProcessor

## How to run the application

### Vorbereitungen
Um die Prozesse der Datenverarbeitung dieser Applikation nutzen zu können, sind folgende Schritte vorher nötig:
1. Die Datenbank Dumps 'page.sql', 'categorylinks.sql' und 'pages-articles-multistream.xml' von Wikipedia unter dem Link https://dumps.wikimedia.org/enwiki/ downloaden und die ersten beiden müssen in eine Datenbank importiert werden.
2. Nach dem Import der sql-Dateien diese als csv-Datei mit Hilfe von SQL exportieren
3. Um die Artikeltexte aus der xml-Datei generieren zu können wird das Programm 'Wiki Parser' (https://dizzylogic.com/wiki-parser/) benötigt.
   Nach der Installation des Programms, kann die xml-Datei sowie der Speicherort ausgewählt werden. Bei den Parsing options sollte "Skip image captions in plain text" angewählt sein.
   Anschließend kann der Export gestartet werden. Dies dauert ca. 3 Stunden. Von den generierten Dateien wird nur 'articles_in_plain_text.txt' benötigt.

### Methoden ausführen
Sind die oben beschriebenen Vorbereitungen getroffen, müssen nun zwei Pfadkonstanten in der Klasse 'PathConstants' (src/main/java/) angepasst werden.
Dabei soll die Konstante 'CSV_DIRECTORY' den Pfad angeben, wo sich die 'page.csv' und 'categorylinks.csv# Dateien befinden.
Die Konstante 'ARTICLE_TEXT_DIRECTORY' soll den Pfad angeben, wo sich die 'articles_in_plain_text.txt' Datei befindet.

Wurden diese Anpassungen gemacht, kann nun die Klasse 'DumpProcessorTest' (src/test/java/) geöffnet werden. Die dort definierten Methoden müssen in folgender Reihenfolge ausgeführt werden:
1. testCSVDumperAllData() >> Durch diese Methode werden die 'page.csv' und 'categorylinks.csv' gejoined und als neue Datei 'page_categorylinks_joined.csv' exportiert
2. testCSVDumperArticleTextAsCsv() >> Durch diese Methode werden die Artikeltexte in 'articles_in_plain_text.txt' nochmals gesäubert und anschließend als 'article_texts.csv' gespeichert
3. testDumpProcessor() >> Mit dieser Methode werden die Artikel, die eine Relation zu der Kategorie 'Animals' aufweisen, rekursiv (mit einer Rekursionstiefe von 2) ermittelt und in Relation zur Rekursionstiefe in die Datei 'level_article_title.csv' gespeichert
   (Info: Hierbei wird mit einem Dataset von Spark gearbeitet, aber durch die Rekursion kann das Verfahren nicht effizient gestaltet werden. Deswegen kann diese Methode bis zu 3-4 Stunden dauern)
4. testCSVDumperNeededTitlesWithContent() >> Durch diese Methode werden die benötigten Artikeltexte ermittelt und als 'needed_article_titles_and_contents.csv' gespeichert

Damit sind alle benötigten Schritte zur Datenanalyse durchgeführt.

### Datenanalyse und Visualisierung mit Python
Anitas Text


## Hypothese und Diskussion der Ergebnisse

### Hypothese

### Leitfragen und Antworten

### Threats to validity

### Future work

## Bestehende Probleme bei der Implementation
 