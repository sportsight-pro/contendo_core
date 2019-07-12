
--------------------------------

base_all_questions_query = """
WITH
  metricDefinitions AS (
  SELECT
    name as metricDefKey,
    metricname as metricKey,
    type as questionType,
    DifficultyLevel as MetricDifficulty,
    Season as seasonsForQ,
    StatLevel as statLevelsForQ,
    StatPeriod as statPeriodsForQ,
    Sports as sportsForQ,
    startQ,
    endQ
  FROM
    `heed-sports.{metricsDefinitionTable}`
  WHERE
    metricname IS NOT NULL
    AND type IN ({questionTypes})
  ),
  matchDetails AS ({MatchinfoQuery}),
  metrics1_pre AS (
  SELECT
    DENSE_RANK() OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey order by m1.MetricValue desc) AS packDenseRank,
    #COUNT (*) OVER (rows between unbounded preceding and unbounded following) as totalItemsCount,
    COUNT(*) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey) AS itemsCount,
    MIN(m1.MetricValue) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey) AS minValue,
    MAX(m1.MetricValue) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey) AS maxValue,
    CASE
        WHEN StatLevel='team' THEN m1.TeamName
        WHEN StatLevel='player' THEN FORMAT("%s (%s)", m1.playername, m1.teamname)
        WHEN StatLevel='match' THEN matchDetails.MatchFullDesc
    END as ObjectName,
    metricDefinitions.*,
    m1.*,
    matchDetails.*
  FROM
    `heed-sports.HEED_History.All_historic_metrics_v4` m1
  LEFT JOIN
    metricDefinitions
  ON
    metricname=metricKey
  LEFT JOIN
    matchdetails
  ON
    matchId=id
  WHERE
    TRUE
    AND MetricDifficulty IS NOT NULL
    AND MetricDifficulty <= {maxDifficulty} 
    AND {MetricCondition}
    AND PlayerId is not null
    AND MetricValue>0
    AND SeasonId IN ({seasonId})
    AND LeagueCode = '{leagueCode}'
    AND STRPOS(statLevelsForQ, StatLevel) > 0 
    AND STRPOS(statPeriodsForQ, StatPeriod) > 0 
    AND STRPOS(sportsForQ, Sport) > 0 
    ),
  metrics1 as (
  SELECT 
    COUNT(*) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey, packDenseRank) AS rankItemsCount,
    ABS(maxValue-minValue) as valueRange,
    * 
  FROM 
    metrics1_pre
  WHERE
    maxValue-minValue>0
  ),
  metrics2 AS (
  SELECT
    MAX(packDenseRank) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey) AS nRanks,
    #MAX(DenseRank) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey) AS orignRanks,
    *
  FROM
    metrics1),
  questions AS (
  SELECT
    #COUNT(*) OVER (PARTITION BY metrics1.StatLevel, metrics1.StatPeriod, metrics1.LeagueCode, metrics1.SeasonId, metrics1.PeriodId, metrics1.metricDefKey) AS totalQuestionsCount,
    COUNT(*) OVER (rows between unbounded preceding and unbounded following) AS totalQuestionsCount,
    metrics1.itemsCount,
    metrics2.nRanks,
    metrics1.MetricName,
    metrics1.metricDefKey,
    metrics1.StatLevel,
    metrics1.StatPeriod,
    metrics1.SeasonId,
    metrics1.PeriodId,
    metrics1.LeagueCode,
    metrics1.packDenseRank as DenseRank,
    metrics1.PercentRank,
    metrics1.MetricDifficulty,
    metrics1.questionType,
    metrics1.startQ,
    metrics1.endQ,
    metrics2.packDenseRank-metrics1.packDenseRank AS rankDiff,
    if (metrics2.nRanks>=5 and metrics1.itemsCount>=40, metrics2.nRanks*0.8, metrics2.nRanks) as maxRank,
    if (metrics1.questionType='teammatch' and (metrics1.matchid != metrics2.matchid or metrics1.homeScore-metrics1.awayScore>1), true, false) as filterOut,
    ROUND(
      LOG10(metrics2.nranks)*2 + 
      LOG10(metrics2.itemsCount)*2 + 
      LOG10(metrics1.rankItemsCount*metrics2.rankItemsCount) +
      IF (metrics1.packDenseRank<metrics2.nRanks/5, 
          metrics1.packDenseRank/metrics2.nranks*50,
          10+metrics1.packDenseRank/metrics2.nranks*5) +
      (1-ABS(metrics1.MetricValue-metrics2.MetricValue)/metrics2.valueRange)*30 +
       metrics1.MetricDifficulty*25.1234
    ,3) AS difficulty,
    (
    SELECT
      AS STRUCT metrics1.*) AS metrics1,
    (
    SELECT
      AS STRUCT metrics2.*) AS metrics2
  FROM
    metrics1
  LEFT JOIN
    metrics2
  ON
    metrics1.metricDefKey=metrics2.metricDefKey
    AND metrics1.packDenseRank<metrics2.packDenseRank
    AND metrics1.LeagueCode=metrics2.LeagueCode
    AND metrics1.PeriodId=metrics2.PeriodId
    AND metrics1.StatLevel=metrics2.StatLevel
    AND metrics1.StatPeriod=metrics2.StatPeriod
    AND metrics1.seasonid=metrics2.seasonid
  WHERE
    metrics2.metricname IS NOT NULL
    AND metrics2.nranks>=2
    AND (ABS(metrics2.MetricValue-metrics2.minValue+1)/(metrics2.valueRange) > 0.1 or metrics2.nRanks<6)
  ),

  questions_w_numbers AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey order by Difficulty) AS rowNum,
    COUNT(*) OVER (PARTITION BY StatLevel, StatPeriod, LeagueCode, SeasonId, PeriodId, metricDefKey) AS questionsCount,
    *
  FROM
    questions
  WHERE
    TRUE
    AND not filterOut
    AND {QuestionCondition}
    ),
  questions_w_rank AS (
  SELECT
    CEIL(rowNum/GREATEST(CEIL(questionsCount/(endQ-startQ+1)),2.0))+startQ-1 as qNum,
    *
  FROM
    questions_w_numbers)

{finalQuery}
"""
final_queries = {
    'final_questions': """
    SELECT
        qNum,
        MetricName,
        MetricDefKey,
        '{packId}' as packId ,
        StatLevel,
        StatPeriod,
        SeasonId,
        PeriodId,
        Difficulty,
        MetricDifficulty,
        questionType,
        rowNum,
        #DifficultyLevel,
        metrics1.MatchFullDesc,
        metrics1,
        metrics2
    FROM
      questions_w_rank
    #ORDER BY
    #  qNum,
    #  difficulty
""",
    'metric_counters': """
      SELECT
        metricDefKey,
        statlevel,
        MAX(MetricDifficulty) as MetricDifficulty,
        MAX(questionType) as questionType,
        '{packId}' as packId ,
        MAX(metrics2.packdenserank) AS nranks,
        MAX(metrics2.itemscount) AS nItems,
        count (*) nFilteredQuestions,
        max(questionsCount) as questionsCount,
        max(totalQuestionsCount) as totalQuestionsCount
      FROM
        questions_w_rank
      GROUP BY
        1,
        2
      ORDER BY
        MetricDifficulty,
        nFilteredQuestions
""",
    'question_counters': """
      SELECT
        '{packId}' as packId ,
        qNum,
        count (*) nFilteredQuestions,
        max(questionsCount) as questionsCount,
        max(totalQuestionsCount) as totalQuestionsCount,
        string_agg(distinct format("%d", MetricDifficulty), ", ") as MetricDifficulties,
        COUNT(DISTINCT metricDefKey) nmetrics,
        count(distinct format("(%s, %s)", metricDefKey, statlevel)) as nMtericsCombinations,
        STRING_AGG(DISTINCT metricDefKey, ",  ") as metrics
        #,string_agg(distinct format("(%s, %s)", metricDefKey, statlevel), ", ") as metricsComb
      FROM
        questions_w_rank
      GROUP BY
        qNum
      ORDER BY
        qNum
""",
}
matchinfo_query = {
    'Soccer': """
        SELECT
            *, 
            FORMAT("%s vs. %s (%d:%d)",homeTeamName, awayTeamName, homeScore, awayScore) as MatchFullDesc
        FROM (
            SELECT
                matchInfo.id,
                #matchInfo.date,
                (
                SELECT
                  shortName
                FROM
                  UNNEST (matchinfo.contestant)
                WHERE
                  position='home') AS homeTeamName,
                (
                SELECT
                  shortName
                FROM
                  UNNEST (matchinfo.contestant)
                WHERE
                  position='away') AS awayTeamName,
                liveData.matchDetails.scores.total.home AS homeScore,
                liveData.matchDetails.scores.total.away AS awayScore
            FROM
                `heed-sports.HEED_History.ma3_match_events_all`)
    """,
    'Basketball': """
        SELECT
            *, 
            FORMAT("%s vs. %s (%d:%d)",homeTeamName, awayTeamName, homeScore, awayScore) as MatchFullDesc
        FROM (
            SELECT
                CAST(gamecode AS STRING) AS id,
                #maindata.date,
                maindata.CodeTeamA as homeTeamName,
                maindata.CodeTeamB as awayTeamName,
                maindata.ScoreA AS homeScore,
                maindata.ScoreB AS awayScore
            FROM
                `heed-sports.euro_league.header`) 
    """,
    'MMA': """
        SELECT
          FORMAT("%s vs. %s",homeTeamName, awayTeamName) as MatchFullDesc,
          *
        FROM (
          SELECT
            CAST(Fights.FightID AS STRING) AS id,
            #FMLiveFeed.Date AS date,
            f1.FullName AS homeTeamName,
            f2.FullName AS awayTeamName,
            Fights.ORDER AS homeScore,
            0 AS awayScore
          FROM
            `heed-sports.ufc.FNT_JSONs`,
            UNNEST (FMLiveFeed.Fights) Fights
          INNER JOIN
            Fights.Fighters f1
          ON
            FightId = FightId
            AND f1.outcome = 'Win'
          INNER JOIN
            Fights.Fighters f2
          ON
            FightId = FightId
            AND f1.FighterId != f2.FighterId
            AND f2.outcome = 'Loss')
    """
}

read_all_questions_queries = {
    'filtered': """
        WITH
          metricscount AS(
          SELECT 
            FLOOR(nFilteredQuestions/{filterRatio}) AS filterFactor,
            FLOOR(nFilteredQuestions/{filterRatio}*RAND()) AS itemToSelect,
            *
          FROM
            `heed-sports.HEED_Questions_dashboard_tables.metric_counters_{packId}`           
          ),
          q1 AS (
          SELECT
            q.*,
            m.filterFactor,
            m.itemToSelect
          FROM
            `heed-sports.HEED_Questions_dashboard_tables.final_questions_{packId}` q
          LEFT JOIN
            metricscount m
          ON
            q.metricDefKey = m.metricDefKey
            AND q.statlevel = m.statlevel),
          q2 AS (
          SELECT
          IF
            (filterFactor=0,
              TRUE,
            IF
              (MOD(rowNum,CAST(filterFactor AS int64))=itemToSelect,
                TRUE,
                FALSE)) AS remains,
            *
          FROM
            q1)
        SELECT
          *
        FROM
          q2
        WHERE
          remains
        ORDER BY
          qnum,
          difficulty
    """,
    'all': "SELECT * FROM `HEED_Questions_dashboard_tables.final_questions_{packId}` order by qnum,difficulty"
}

player_condition = """
    '{}' in (metrics1.playerid, metrics2.playerid)
    and '{}' in (metrics1.playerid, metrics2.playerid)
"""
team_condition = """
    '{}' in (metrics1.teamid, metrics2.teamid)
    #and metrics1.teamid != metrics2.teamid
"""
oneteam_condition = """
    '{}' in (metrics1.teamid, metrics2.teamid)
    and metrics1.teamid = metrics2.teamid
"""
teams_condition = """
    '{}' in (metrics1.teamid, metrics2.teamid)
    and '{}' in (metrics1.teamid, metrics2.teamid)
"""
teams_only_condition = team_condition + """
    and 'none' in (metrics1.playerid, metrics2.playerid)
"""
# matchid_condition = "    matchId='{}' and metrics1.teamid!=metrics2.teamid"


match_full_desc_query = {
    "Soccer": 'FORMAT("%s vs. %s (%d:%d)",metrics1.homeTeamName, metrics1.awayTeamName, metrics1.homeScore, metrics1.awayScore)',
    "Basketball": 'IF (metrics1.id IS NOT NULL, FORMAT("%s (%d:%d)",metrics1.MatchDesc, metrics1.homeScore, metrics1.awayScore),"") ',
    "MMA": 'metrics1.matchDesc ',
}

------------------------------

import random
import math
from itertools import combinations
import matplotlib.pyplot as plt


#
# main class responsible for generating the questionnaires.
class questionnaire_generator():
    #
    # read in the configurations
    def __init__(self):
        difficulty_df = pd.read_csv(
            'https://docs.google.com/spreadsheets/d/1VQE0wNFdcaMBuLxLuIYaQuM4XHVQfKozHQl6nzxLeKI/export?format=csv&gid=1297329538').fillna(
            '')
        asset_df = pd.read_csv(
            'https://docs.google.com/spreadsheets/d/1VQE0wNFdcaMBuLxLuIYaQuM4XHVQfKozHQl6nzxLeKI/export?format=csv&gid=1974614086').fillna(
            '')
        pack_generation_df = pd.read_csv(
            'https://docs.google.com/spreadsheets/d/1VQE0wNFdcaMBuLxLuIYaQuM4XHVQfKozHQl6nzxLeKI/export?format=csv&gid=2020763987').fillna(
            '')

        self.general_translation_df = pd.read_csv(
            'https://docs.google.com/spreadsheets/d/1VQE0wNFdcaMBuLxLuIYaQuM4XHVQfKozHQl6nzxLeKI/export?format=csv&gid=1080103404').fillna(
            '')
        self.asset_df = pd.read_csv(
            'https://docs.google.com/spreadsheets/d/1VQE0wNFdcaMBuLxLuIYaQuM4XHVQfKozHQl6nzxLeKI/export?format=csv&gid=1974614086').fillna(
            '')
        self.difficultyDict = df_to_dictionary(difficulty_df, 'DifficultyLevel')
        self.packDict = df_to_dictionary(pack_generation_df, 'packId')
        self.pack_sport_df = {}
        self.triviaDict = {}
        self.language_columns = ['de-DE', 'pt-PT']

        for packId, packDef in self.packDict.items():
            if packDef['packType'] == 'simple':
                sheet = 'https://docs.google.com/spreadsheets/d/1VQE0wNFdcaMBuLxLuIYaQuM4XHVQfKozHQl6nzxLeKI/export?format=csv&gid={}'.format(
                    int(packDef['triviaConfigSheet']))
                # display(sheet)
                self.pack_sport_df[packId] = pd.read_csv(sheet)
                self.triviaDict[packId] = df_to_dictionary(self.pack_sport_df[packId], 'name')

        self.translation_general = {}
        for i, transline in self.general_translation_df.iterrows():
            for key, value in transline.items():
                if key == 'Code':
                    self.translation_general[value] = {}
                    current_code = value
                else:
                    self.translation_general[current_code][key] = value
        self._all_questions = None
        self.qdf_groups = None

    #
    # helper: comma separated list to list-like
    def str_to_liststr(self, inStr):
        _res = str(inStr.split(',')).replace('[', '').replace(']', '')
        return _res

        #

    # get the relevant query
    def save_pack_questions_to_bq(self, inpackId=None):
        _start_time = dt.now()
        query_jobs = {}
        table_refs = {}
        for packId in self.packDict.keys():
            packDef = self.packDict[packId]
            if inpackId is not None:
                if packId != inpackId:
                    continue
            try:
                display('Starting {}'.format(packId))
                _bigquery_client = bigquery.Client()
                #
                # converting the metric definitions CSV to a BQ table to be used in the query afterwards.
                _metricsDefinitionTable = 'HEED_Questions_temps.pack_config_{}_{}_{}'.format(packDef['Sport'], packId,
                                                                                             int(packDef[
                                                                                                     'triviaConfigSheet']))
                _pack_sport_df_keys = self.pack_sport_df[packId].keys()[0:10]
                _pack_sport_df = self.pack_sport_df[packId][_pack_sport_df_keys]
                #
                # get the difficulty ranges and insert to the pack
                difficultyRanges = eval(packDef['difficultyRanges'])
                _pack_sport_df['Sports'] = packDef['Sport']
                _pack_sport_df['startQ'] = _pack_sport_df.apply(lambda x: difficultyRanges[x['DifficultyLevel']][0],
                                                                axis=1)
                _pack_sport_df['endQ'] = _pack_sport_df.apply(lambda x: difficultyRanges[x['DifficultyLevel']][1],
                                                              axis=1)
                #
                # replace the statperiod if defined.
                if len(packDef['StatPeriod']) > 0:
                    _pack_sport_df['StatPeriod'] = packDef['StatPeriod']
                #
                # Writing to BQ.
                _pack_sport_df.to_gbq(
                    _metricsDefinitionTable,
                    project_id='heed-sports',
                    if_exists='replace'
                )
                display('Saved table {}, delta time {}'.format(_metricsDefinitionTable, dt.now() - _start_time))
                #
                # Defining the instruction parameters for the query and formatting the query.
                _sport = packDef['Sport']
                instructions = {
                    'sport': _sport,
                    'MatchinfoQuery': matchinfo_query[_sport],
                    'MatchFullDesc': match_full_desc_query[_sport],
                    'maxDifficulty': int(packDef['maxDifficulty']),
                    'leagueCode': packDef['leaguecode'],
                    'seasonId': self.str_to_liststr(packDef['seasonid']),
                    'metricsDefinitionTable': _metricsDefinitionTable,
                    'MetricCondition': eval(packDef['MetricCondition']),
                    'QuestionCondition': eval(packDef['QuestionCondition']),
                    # 'DifficultyInstructions': DifficultyInstructions, #self.calculate_difficulty_levels_Query(),
                    'packId': packId,
                    'questionTypes': self.str_to_liststr(packDef['questionTypes']),
                    'finalQuery': '',
                }
                queries = ['final_questions', 'metric_counters', 'question_counters']
                all_questions_query = {}
                for finalQuery in queries:
                    dataset_id = 'HEED_Questions_dashboard_tables'
                    table_id = '{}_{}'.format(finalQuery, packId)
                    job_config = bigquery.QueryJobConfig()
                    # Set the destination table
                    table_refs[table_id] = _bigquery_client.dataset(dataset_id).table(table_id)
                    job_config.destination = table_refs[table_id]
                    job_config.write_disposition = 'WRITE_TRUNCATE'
                    instructions['finalQuery'] = final_queries[finalQuery].format(**instructions)
                    all_questions_query[table_id] = base_all_questions_query.format(**instructions)
                    # print(all_questions_query[table_id])
                    #
                    # running the questions query
                    query_jobs[table_id] = _bigquery_client.query(all_questions_query[table_id], job_config=job_config)
                display('End calculating questions for {}, delta-time: {}'.format(packId, dt.now() - _start_time))
            except Exception as e:
                display('Error with pack {}, exception: {} '.format(packId, e))
            continue
        for table_id in query_jobs.keys():
            query_jobs[table_id].result()  # Waits for the query to finish
            display('Query results loaded to table {}, Delta-time: {}'.format(table_refs[table_id].path,
                                                                              dt.now() - _start_time))

    def get_questions_df_by_query(self, packId, filterRatio=None):
        inst = {'packId': packId, 'filterRatio': filterRatio}
        if filterRatio is None:
            queryType = 'all'
        else:
            queryType = 'filtered'
        questions_query = read_all_questions_queries[queryType].format(**inst)

        _bigquery_client = bigquery.Client()
        _start_time = dt.now()
        print('Start reading {}'.format(packId))
        questions_query_job = _bigquery_client.query(questions_query)
        questions_df = questions_query_job.result().to_dataframe().fillna('')
        print('Got {} resultes, Delta-time: {}'.format(questions_df.shape, dt.now() - _start_time))
        return questions_df

    #
    # read in all questions for this pack
    def read_all_questions(self, packId, filterRatio=None, nquestions=None):
        packDef = self.packDict[packId]
        if nquestions is None:
            nquestions = int(packDef['numQuestions'])
        questions_df = self.get_questions_df_by_query(packId, filterRatio)
        #
        # Preparing the grouping of the questions for the questions generation.
        dflen = questions_df.shape[0]
        sectionSize = int(dflen / nquestions)
        self.qdf_groups = {}
        for i in range(0, nquestions):
            if i == 0:
                qRange = [0, 1]
            else:
                qRange = [i + 1]
            secdf = questions_df.query('qNum in {}'.format(qRange))
            self.qdf_groups[i] = secdf.groupby(['MetricDefKey', 'StatLevel']).groups
            print('qNum={}, nItems={}, metrics={}'.format(i + 1, len(secdf), len(self.qdf_groups[i].keys())))

        display('Calculated QuestionNum {}, Delta-time: {}'.format(questions_df.shape, dt.now() - start_time))

        self._all_questions = questions_df

    #
    # utility to add a record to a dict object
    def add_dict_as_a_record(self, main, record, keys, suffix=''):
        for key in keys:
            main['{}{}'.format(key, suffix)] = record[key]
        return main

    #
    # helper function to get the random asset spreadsheet index.
    def randomize_asset_entry(self, packId):
        query_df = self.asset_df.query('PackId == "{}"'.format(self.packDict[packId]['AssetPack']))
        assets = list(query_df['Count'])
        return assets[random.randint(0, len(assets) - 1)]
        #

    # helper function to get the random asset for question.
    def get_random_asset(self, packId):
        index = self.randomize_asset_entry(packId)
        query_df = self.asset_df.query('Count == {}'.format(index))
        return {
            'type': query_df.iloc[0]['AssetType'],
            'assetId': query_df.iloc[0]['AssetId']
        }

    #
    # drop the parameters into the string template based on the instruction dictionary
    def update_string_by_instructions_with_translation(self, inst, base_string, lan):
        if type(base_string) != str:
            return ''
        outstring = base_string
        for key, value in inst.items():
            if value in self.translation_general.keys():
                outstring = outstring.replace('{' + key + '}', str(self.translation_general[value][lan]))
            else:
                outstring = outstring.replace('{' + key + '}', str(value))
        return outstring

    #
    # generate a translate-table from a line (all languages)
    def update_translate_table(self, id, transTable, row, inst={}):
        for lan in self.language_columns:
            if transTable.get(lan) is None:
                transTable[lan] = {}
            transTable[lan][id] = self.update_string_by_instructions_with_translation(inst, row[lan], lan)

    #
    # generate a text-def Zemingo knows to read
    def generate_textdef(self, defaultText, localizationId):
        return {
            'defaultText': defaultText,
            'localizationId': localizationId
        }

    #
    # generating the Firbase and BigQuery question structures
    def generate_question_fb_and_bq_objects(self, _row, packId, questionNum):
        _metrics_def = self.pack_sport_df[packId]
        _metric_def = _metrics_def[_metrics_def['name'] == _row['MetricDefKey']].iloc[0]
        # display(_metric_def)

        transTable = {}
        #
        # generating the question
        questionInst = {
            'WhichTeamOrPlayerOrMatch': _row['StatLevel'],
            'matchDesc': _row['MatchFullDesc'],
            'period': _row['PeriodId'],
            'match': "{}{}".format(_row['MatchFullDesc'],
                                   "" if _row['StatPeriod'] in ['Season2Date', 'All-Season', "Season"] else " "),
            'season': _row['SeasonId'],
            'TimeFrame': self.packDict[packId]['TimeFrame'],
        }

        processedQuestion = self.update_string_by_instructions_with_translation(questionInst,
                                                                                _metric_def['Question'],
                                                                                'Default')
        processedQuestion = processedQuestion.format(**questionInst)

        question = self.generate_textdef(processedQuestion, 'question')
        #
        # updating the translation-table
        self.update_translate_table('question', transTable, _metric_def, questionInst)
        v1 = _row['metrics1']['MetricValue']
        v2 = _row['metrics2']['MetricValue']
        blank = ' '
        _objectName1 = _row['metrics1']['ObjectName']
        _objectName2 = _row['metrics2']['ObjectName']
        answer1 = {
            'id': 'answer-1',
            'answerTitle': self.generate_textdef(_objectName1, 'answer1'),
            'answerValue': self.generate_textdef(eval(_metric_def['answer1format']), 'Value1'),
        }
        answer2 = {
            'id': 'answer-2',
            'answerTitle': self.generate_textdef(_objectName2, 'answer2'),
            'answerValue': self.generate_textdef(eval(_metric_def['answer2format']), 'Value2'),
        }
        #
        #
        row_keys = ['MatchFullDesc', 'Difficulty', 'MetricDefKey', 'MetricDifficulty']
        specific_metric_keys = ['packDenseRank', 'itemsCount', 'MetricValue', 'STDDEV', 'Count', 'Rank', 'DenseRank',
                                'PercentRank', 'MatchFullDesc', 'TeamId', 'TeamName', 'PlayerId', 'PlayerName']
        common_metric_keys = ['MetricName', 'Description', 'questionType', 'League', 'LeagueCode', 'SeasonId',
                              'TournamentRound', 'MatchId', 'PeriodId', 'StatType', 'StatLevel', 'StatPeriod']
        bigqueryQuestion = {
            'num': questionNum,
            'question': question['defaultText'],
            'answer1Title': _objectName1,
            'answer1Value': answer1['answerValue']['defaultText'],
            'answer2Title': _objectName2,
            'answer2Value': answer2['answerValue']['defaultText'],
            'correctAnswerId': 1,
        }

        bigqueryQuestion = self.add_dict_as_a_record(bigqueryQuestion, _row, row_keys, '')
        bigqueryQuestion = self.add_dict_as_a_record(bigqueryQuestion, _row['metrics1'], common_metric_keys, '')
        bigqueryQuestion = self.add_dict_as_a_record(bigqueryQuestion, _row['metrics1'], specific_metric_keys, '1')
        bigqueryQuestion = self.add_dict_as_a_record(bigqueryQuestion, _row['metrics2'], specific_metric_keys, '2')

        firebaseQuestion = {
            'question': question,
            'answers': [answer1, answer2],
            'correctAnswerId': 'answer-1',
            # 'relevantMomentsIds': [self.get_moment_by_pack(packDef, first_metric), self.get_moment_by_pack(packDef, second_metric)],
            'backgroundImage': self.get_random_asset(packId),
            'translationTable': transTable,
            'metricName': _row['MetricDefKey'],
            'questionTemplateType': _metric_def['type'],
            'num': int(questionNum),
            'difficultyLevel': int(_row['MetricDifficulty']),
        }
        return firebaseQuestion, bigqueryQuestion

    def generate_one_question(self, questionNum, packId):
        packDef = self.packDict[packId]
        if packDef['orderMethod'] in ['category', 'range']:
            #
            # find the first category with relevant values
            keys = {}
            qn = questionNum
            while len(keys) == 0:
                qn -= 1
                keys = self.qdf_groups[qn].keys()
            #
            # generate all available combinations
            _unseen_combinations = set(keys) - self._seen_combinations
            while not bool(_unseen_combinations):  # run out of combinations
                self._seen_combinations = set()  # reset combinations
                _unseen_combinations = set(keys) - self._seen_combinations
            #
            # pick one key from all available
            key = random.sample(_unseen_combinations, 1)[0]
            group = self.qdf_groups[qn][key]
            index = group[random.randint(0, len(group) - 1)]
            _one_question = dict(self._all_questions.iloc[index])
            self._seen_combinations.add(key)  # add the seen combination to the set
        elif packDef['orderMethod'] == 'serial':
            _one_question = dict(self._all_questions.iloc[questionNum - 1])
        else:
            return None, None

        fbQuestion, bqQuestion = self.generate_question_fb_and_bq_objects(_one_question, packId, questionNum)
        return fbQuestion, bqQuestion

    def generate_questionnaire(self, packId, nquestions=0):
        if nquestions == 0:
            _num_question = int(self.packDict[packId]['numQuestions'])
        else:
            _num_question = nquestions
        # generate questinnaire Id
        q_id = 'Q-{}-{}'.format(packId, str(int(dt.timestamp(dt.now()) * 1000)))
        # 'Q-'+str(int(dt.timestamp(dt.now())*1000))

        # place holder for questions
        fbQuestionList = []
        bqQuestionList = []
        self._seen_combinations = set()

        # increment questionNumber and iterate through instruction list
        _questionNum = 1
        _instruction_counter = 0
        while _questionNum <= _num_question:
            # _inst = _all_instruction_type[_instruction_counter%len(_all_instruction_type)] # pick one instruction
            fbQuestion, bqQuestion = self.generate_one_question(_questionNum,
                                                                packId)  # generate one question based on instruction

            if fbQuestion is None or bqQuestion is None:
                _instruction_counter += 1  # if no question available, next instruction
                continue
            #
            # add question to the list
            fbQuestionList.append(fbQuestion)
            bqQuestionList.append(bqQuestion)

            # nest question
            _questionNum += 1
            _instruction_counter += 1
        return q_id, fbQuestionList, bqQuestionList


------------------------------------------------------------------------------

## # write to firebase and BQ of the games tree.
# all packs, 5 questionnaire per pack.
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

# Initialize the app with a service account, granting admin privileges
# Fetch the service account key JSON file contents
destinations = {'Dev': 'sports', 'Prod': 'c99f6'}
credential_list = {'Dev': '../credentials.json', 'Prod': '../gcp-ysherman-cred.json'}
destination = 'Prod'
# destination='Dev'
try:
    if not first:
        display('already connected to {}'.format(destinations[destination]))
except:
    display('Connecting {}'.format(destination))
    cred = credentials.Certificate(credential_list[destination])
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://heed-{}.firebaseio.com/'.format(destinations[destination])
    })
    first = False

#
# updating the node in firebase.
qg = questionnaire_generator()

start_time = dt.now()
for packId in qg.packDict.keys():
    packDef = qg.packDict[packId]
    if packId not in ['HummlesBUN']:
        continue
    try:
        display('Starting {}'.format(packId))
        # qg.save_pack_questions_to_bq(packId)
        display('End calculating questions for {}, delta-time: {}'.format(packId, dt.now() - start_time))
    except Exception as e:
        display('Error with pack {}, exception: {} '.format(packId, e))
    # continue

    numGames = packDef['numQuestionnaire']
    numQuestions = packDef['numQuestions']
    if numGames <= 0:
        continue

    display(packDef)
    questions_df = pd.DataFrame()
    qg.read_all_questions(packId, nquestions=numQuestions)
    #
    # reset the packs
    path = '/v4/public/testYourself/games/{}/'.format(packId)
    ref = db.reference(path)
    # ref.set({})
    #
    # create the qeustionnaires for the pack
    for i in range(0, numGames):
        id, fbql, bqql = qg.generate_questionnaire(packId, numQuestions)
        q_df = pd.DataFrame(bqql)
        q_df['QuestionnaireId'] = id
        questions_df = questions_df.append(q_df)
        path = '/v4/public/testYourself/games/{}/{}/'.format(packId, i)
        ref = db.reference(path)
        # display(path,fbql)
        ref.set({'id': id, 'questions': fbql})
        if (i + 1) % (max(numGames / 20, 5)) == 0:
            display('{}, {}, delta-time: {}'.format(packId, i + 1, dt.now() - start_time))
        #
        # save the combine data data to BQ.
        if (i + 1) % 250 == 0 or i + 1 == numGames:
            tableName = 'HEED_Questions_Auto_V4.quetions_{}_{}_{}'.format(destination, packDef['Sport'],
                                                                          dt.now().strftime('%Y%m%d'))
            display('Start Write to Bigquery table {},shape: {}, delta-time: {}'.format(tableName, questions_df.shape,
                                                                                        dt.now() - start_time))
            questions_df['packId'] = packId
            questions_df['Sport'] = packDef['Sport']
            questions_df['timestamp'] = dt.now()
            questions_df.to_gbq(
                tableName,
                project_id='heed-sports',
                if_exists='append'
            )
            display('End Write to Bigquery table {},shape: {}, delta-time: {}'.format(tableName, questions_df.shape,
                                                                                      dt.now() - start_time))
            questions_df = pd.DataFrame()

# display(ref.get('packs'))
'done'