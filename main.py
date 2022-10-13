# prototyping only
from src.searchbic import SearchBIC
from src.tftext import TFText
searches = ['data science', 'python', 'decision science', 'data analyst']
bic = SearchBIC(searches).update()
bic.report()
jon = TFText(bic.df)
