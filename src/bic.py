from .stripper import MLStripper
from .url import url_base
from .tftext import TFText
from io import StringIO
from os.path import exists
from pandas import json_normalize
from time import sleep
import cloudpickle
import numpy as np
import pandas as pd
import re
import requests

# NOTE: Needs documentation and refactor

class SearchBIC:
    """
    TODO

    ...

    Attributes
    ----------
    search_terms: list
        a list of search term text

    Methods
    -------
    update():
        TODO

    report(days):
        run a report for newest results

    save():
        manually save data
    """

    def __init__(self, search_terms):
        """
        Takes: search_terms: list
        Search a bic

        """
        self._report_cols = ['title_company', 'title', 'search_term', 'experience_level', 'date', 'body_nothml']
        self._de_dup_cols = self._report_cols + ['body_summary']
        self.search_terms = search_terms
        self.per_page = 100
        self.page = 1
        self.history_filename = 'data/bic_history.cloudpickle'
        self._loadhistory()
        self._cleanhtml()
        self._today_only_limit = 10
        self.industry_filter = ['508', '509', '511']
        self.company_filter = ['Capital One']
        self.TFText = TFText()

        


    def __doc__(self):
        pass

    def __str__(self):
        return f"SearchBIC: search_terms='{self.search_terms}')"

    def __repr__(self):
        return "SearchBIC()"

    def _cleanhtml(self):
        """

        """
        self.data['data']['body_nothml'] = self.data['data'].body.apply(MLStripper.strip_tags)


    def _getnewdata(self):
        """
        fetch and dedup
        """
        if self.df.shape[0] > 0:
            records_before = self.df.shape[0]
        title_maps = self.data['company_group'].set_index('company').to_dict()['group']
        new_df = pd.DataFrame()
        self.search_terms = [x.replace(' ', '+') for x in self.search_terms]
        for st in self.search_terms:
            new_df = pd.concat([new_df, self._searchandpull(st)])
        new_df.index = pd.RangeIndex(new_df.shape[0])
        new_df = new_df.assign(title_company_class=new_df.title_company.map(title_maps))
        his_df = self.data['data']
        if his_df.shape[0] > 0:
            his_df = pd.concat([his_df, new_df])
            his_df.index = pd.RangeIndex(his_df.shape[0])
            for i, c in his_df.iteritems():
                if any([isinstance(x, list) for x in c.values]):
                    his_df[i] = his_df[i].astype(str)
            his_df = his_df.assign(sort_job=pd.to_datetime(his_df.sort_job, utc=True))
            his_df = his_df.sort_values(by='sort_job', ascending=False)
            his_df = his_df.drop_duplicates(subset=self._de_dup_cols, keep='last')
            his_df.index = pd.RangeIndex(his_df.shape[0])
            self.data['data'] = his_df
            self.df = self.data['data']
            if records_before:
                records_after = self.df.shape[0]
                if records_after != records_before:
                    print(f"Fetched {records_after-records_before} new records")
                else:
                    print(f"No new records as of {pd.Timestamp.now().date()}")
        else:
            self.data['data'] = new_df


    def _loadhistory(self):
        """
        load or make new file
        """
        if exists(self.history_filename):
            with open(self.history_filename, "rb") as file_:
                    self.data = cloudpickle.load(file_)
                    self.df = self.data['data']
                    print(f"Loading from {self.history_filename}")
        else:
            _ = {'data': pd.DataFrame(), 'company_group': None}
            if exists('company_group.p'):
                _['company_group'] = pd.read_pickle('company_group.p')
            cloudpickle.dump(_, open(self.history_filename, 'wb'))
            self.data = _

    def _make_url(self, search, per_page, page):
        """
        concatenate url parts

        NOTE: this is silly but reasons
        """
        url_ = url_base.copy()
        url_.append(f'categories=147&')
        url_.append(f'subcategories=&')
        url_.append(f'experiences=&')
        url_.append(f'industry=&')
        url_.append(f'regions=&')
        url_.append(f'locations=3%2C1%2C2%2C48%2C56%2C4&')
        url_.append(f'remote=2&')
        url_.append(f'per_page={per_page}&')
        url_.append(f'page={page}&')
        url_.append(f'search={search}&')
        url_.append(f'sortStrategy=recency&')
        url_.append(f'job_locations=1%7C3%2C1%7C1%2C1%7C2%2C1%7C48%2C1%7C56%2C1%7C4%2C1%7C0&')
        url_.append(f'company_locations=1%7C3%2C1%7C1%2C1%7C2%2C1%7C48%2C1%7C56%2C1%7C4%2C1%7C0&')
        url_.append(f'jobs_board=true&')
        url_.append(f'national=false')
        url = ''.join(url_)
        return(url)


    def _savehistory(self):
        """
        write out log

        """
        print(f"Saving to {self.history_filename}")
        self.data['data'] = self.data['data'].drop_duplicates(subset=self._de_dup_cols, keep='last')
        cloudpickle.dump(self.data, open(self.history_filename, 'wb'))


    def _parsestr2list(self, s):
        """
        for use with pd.apply methods
        """
        brackets_comma = re.compile(',|\[|\]')
        clean_str = brackets_comma.sub('', s)
        return(clean_str.split(' '))


    def _searchandpull(self, search_term, per_page=100):
        """

        """
        j = requests.get(self._make_url(search_term, self.per_page, self.page)).json()
        responses = [j]
        if 'job_all_count' in j.keys():
            pages_remaining = int(np.ceil(j['job_all_count'] / self.per_page))
            for page in range(1, pages_remaining+1):
                if page > 1:
                    responses.append(requests.get(self._make_url(search_term, self.per_page, self.page)).json())
                    sleep(.33)  # NOTE: this needs moved to variable
        jobs_ = [json_normalize(x, record_path='jobs') for x in responses]
        companies_ = [json_normalize(x, record_path='companies') for x in responses]
        jobs_df = pd.concat(jobs_, ignore_index=True)
        companies_df = pd.concat(companies_, ignore_index=True)
        companies_df = companies_df.loc[companies_df.apply(tuple, 1).drop_duplicates().index]  # drop dups
        jobs_df = jobs_df.assign(search_term=search_term)
        jobs_df = jobs_df.assign(date=pd.to_datetime(jobs_df.sort_job))
        out_df = pd.merge(jobs_df, companies_df, left_on='company_id', right_on='id', how='inner', suffixes=('', '_company'))
        out_df = out_df.sort_values(by='date', ascending=False)
        out_df.index = pd.RangeIndex(out_df.shape[0])
        return(out_df)


    def report(self, days:int=0, filtered_view:bool=True):
        """
        print a report to the console
        """
        filter_warning = False
        df = self.df.copy()
        if filtered_view:
            pre_filter_count = df.shape[0]
            for scf in self.industry_filter:
                df = df[df.sub_category_id.apply(lambda x: scf not in self._parsestr2list(x))]
            df = df[df.title_company.apply(lambda x: x not in self.company_filter)]
            post_filter_count = df.shape[0]
            if pre_filter_count != post_filter_count:
                filter_warning = True
        r = df.copy()
        r = r.assign(date=r.date.dt.date)
        r = r.assign(date=pd.to_datetime(r.date))
        r = r[self._report_cols].drop_duplicates()
        r = r[r.body_nothml.ne('')]
        r = r.assign(body_nothml=r.body_nothml.str.strip())
        r = r.assign(body_nothml=r.body_nothml.str[:120])
        r = r[r.date.ge(r.date.max()-pd.Timedelta(days=days))]
        if r.shape[0] > 0:
            for i, gb in r.groupby('search_term'):
                for ii, gbl in gb.groupby('experience_level'):
                    gbl = gbl.drop(['experience_level', 'search_term'], axis=1)
                    print('#####################################################################################')
                    print(f"### {i.replace('+', ' ')}: {ii}")
                    print('#####################################################################################')
                    print(gbl.to_string(index=None, index_names=False, justify='center'))
                    print(' ')
            if filter_warning:
                print(f"Some jobs filtered by category")
        else:
            print(f"nothing new")
        print(f"days: {days}")


    def save(self):
        """

        """
        self._savehistory()


    def update(self):
        """

        """
        self._getnewdata()
        self._savehistory()
        self.data['data'].body_nothml.fillna('', inplace=True)
        self.df = self.data['data']
        return(self)
