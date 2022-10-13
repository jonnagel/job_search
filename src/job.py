class Job(object):
    def __init__(self):
        """

        Takes:
        Returns:
        """
        self.title
        self.text
        self.scores = {}
        self.search_term

    def _parse_job_title(s):
        """
        
        Takes:
        Returns:
        """
        s = s.str.lower()
        s = s.apply(lambda x: ''.join([x if x in list(ascii_lowercase) else ' ' for x in list(x)]))
        while s.str.count('  ').max() > 0:
            s = s.str.replace('  ', ' ')
        words_unique = set([x for y in s.str.split().tolist() for x in y])
        wide_df = pd.DataFrame(columns=list(words_unique), index=s.index)
        wide_df['title'] = s
        for i, t in enumerate(words_unique):
            wide_df.loc[s.str.find(t).ne(-1), t] = True
        wide_df = wide_df.fillna(False)
        return((s, wide_df))
