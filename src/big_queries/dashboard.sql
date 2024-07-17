WITH 
LatestEra AS (

                    SELECT DISTINCT era_id AS last_era_id
                    FROM data.polkadot__stake_eras
                    ORDER BY era_id DESC
                    LIMIT 1
                  ),

LastKEras AS (
                    SELECT DISTINCT era_id
                    FROM data.polkadot__stake_eras
                    ORDER BY era_id DESC
                    LIMIT 30
                  ),

Last365Eras AS (
                    SELECT DISTINCT era_id
                    FROM data.polkadot__stake_eras
                    ORDER BY era_id DESC
                    LIMIT 365
                ),


NominatorRewards AS (
    SELECT
        JSON_EXTRACT_SCALAR(e.event, '$.data[0]') AS nominator_id,
        SAFE_CAST(JSON_EXTRACT_SCALAR(b.metadata, '$.active_era_id') AS INT64) - 1 AS era_id,
        CASE 
            WHEN SAFE_CAST(JSON_EXTRACT_SCALAR(e.event, '$.data[1]') AS FLOAT64) IS NOT NULL 
            THEN SAFE_CAST(JSON_EXTRACT_SCALAR(e.event, '$.data[1]') AS FLOAT64) / 1e10
            WHEN SAFE_CAST(JSON_EXTRACT_SCALAR(e.event, '$.data[2]') AS FLOAT64) IS NOT NULL 
            THEN SAFE_CAST(JSON_EXTRACT_SCALAR(e.event, '$.data[2]') AS FLOAT64) / 1e10
            ELSE NULL
        END AS total_reward
    FROM 
        data.polkadot__events e
    JOIN 
        data.polkadot__blocks b ON e.block_id = b.block_id
    WHERE
        e.method IN ('Rewarded', 'Reward')
),


NominatorStakesFee AS (
    SELECT DISTINCT
        sn.era_id AS era_id,
        sn.account_id AS nominator_id,
        sn.validator AS validator_id,
        sn.value / 1e10 AS nominator_stake,
        CAST(JSON_EXTRACT_SCALAR(sv.prefs, '$.commission') AS FLOAT64) / 1e10 AS validator_fee
    FROM
        data.polkadot__stake_nominators sn
    JOIN
        data.polkadot__stake_validators sv ON sn.validator = sv.account_id AND sn.era_id = sv.era_id
    ORDER BY
        sn.era_id, nominator_id DESC
),

ValidatorStakesFee AS (
    SELECT
        sv.era_id AS era_id,
        sv.account_id AS validator_id,
        sv.own / 1e10 AS validator_stake,                  
        sv.nominators_count,                             
        sv.total / 1e10 AS total_stake,
        rv.reward_points AS reward_points,             
        CAST(JSON_EXTRACT_SCALAR(sv.prefs, '$.commission') AS FLOAT64) / 1e10 AS validator_fee
    FROM
        data.polkadot__stake_validators sv
    LEFT JOIN
        data.polkadot__rewards_validators rv ON sv.account_id = rv.account_id AND sv.era_id = rv.era_id
    ORDER BY
        sv.era_id, sv.account_id DESC
),

ActiveValidatorsLast30 AS (
    SELECT 
        validator_id,
        COUNT(*) AS active_count,
        COUNT(*) * 1.0 / 30 AS success_rate
    FROM 
        ValidatorStakesFee
    WHERE 
        era_id IN (SELECT era_id FROM LastKEras)
    GROUP BY 
        validator_id
),

WasActiveIn30Days AS (
    SELECT
        nsf.nominator_id,
        nsf.validator_id,
        avl30.active_count,
        avl30.success_rate,
        CASE 
            WHEN avl30.active_count > 0 THEN 1
            ELSE 0
        END AS was_active
    FROM
        NominatorStakesFee nsf
    LEFT JOIN
        ActiveValidatorsLast30 avl30 ON nsf.validator_id = avl30.validator_id
),

NominatorValidatorsMetrics AS (
    SELECT
        nominator_id,
        MAX(active_count) AS active_count,
        MAX(success_rate) AS success_rate,
        MAX(was_active) AS was_active
    FROM
        WasActiveIn30Days
    GROUP BY
        nominator_id
),

-- AggregatedData AS (
--     SELECT
--         ns.era_id,
--         ns.nominator_id,
--         SUM(ns.nominator_stake) as nominator_stake,
--         SUM(nr.total_reward) as total_reward
--     FROM
--         NominatorStakesFee ns
--     LEFT JOIN
--         NominatorRewards nr ON nr.nominator_id = ns.nominator_id AND nr.era_id = ns.era_id
--     WHERE ns.era_id IN (SELECT era_id FROM Last365Eras)
--     GROUP BY 
--     ns.era_id, nominator_id
-- ),

AggregatedNominatorStake AS (
    SELECT DISTINCT
        sn.era_id AS era_id,
        sn.account_id AS nominator_id,
        SUM(sn.value / 1e10) AS nominator_stake,  -- Aggregate all stakes for the same nominator_id and era_id
    FROM
        data.polkadot__stake_nominators sn
    GROUP BY
        era_id, nominator_id
),

AggregatedRewards AS (
    SELECT
        era_id,
        nominator_id,
        SUM(total_reward) AS total_reward
    FROM
        NominatorRewards
    GROUP BY
        era_id, nominator_id
),

RewardsAndStakes AS (
    SELECT
        ns.era_id,
        ns.nominator_id,
        ns.nominator_stake,
        COALESCE(nr.total_reward, 0) AS total_reward,
        CASE
            WHEN (nr.total_reward / ns.nominator_stake)*365 > 0.1888 THEN ns.nominator_stake * 0.2 / 365
            ELSE nr.total_reward
        END AS adjusted_reward
    FROM
        AggregatedNominatorStake ns
    LEFT JOIN
        AggregatedRewards nr ON nr.nominator_id = ns.nominator_id AND nr.era_id = ns.era_id
    WHERE ns.era_id IN (SELECT era_id FROM Last365Eras)
),


YearlyReward AS (
    SELECT
        nominator_id,
        SUM(adjusted_reward) AS yearly_total_reward,
        AVG(nominator_stake) AS average_stake
    FROM
        RewardsAndStakes
    GROUP BY
        nominator_id
),

LatestEraStats AS (
    SELECT
        nominator_id,
        total_reward AS last_era_reward,
        nominator_stake AS current_stake
    FROM
        -- AggregatedData
        RewardsAndStakes
    WHERE era_id = (SELECT last_era_id FROM LatestEra)
),


RewardQuantiles AS (
    SELECT
        nominator_id,
        ntile(5) OVER (ORDER BY last_era_reward) AS reward_quantile
    FROM
        LatestEraStats
),

StakeQuantiles AS (
    SELECT
        nominator_id,
        ntile(4) OVER (ORDER BY current_stake) AS stake_quantile
    FROM
        LatestEraStats
),


HighestMinStake AS (
    SELECT
        ns.nominator_id,
        MIN(GREATEST(ns.nominator_stake, 580)) AS highest_min_stake
    FROM
        NominatorStakesFee ns
    WHERE ns.era_id = (SELECT last_era_id FROM LatestEra)
    GROUP BY
        ns.nominator_id
),

CurrentEraFee AS (
    SELECT
        nominator_id,
        AVG(validator_fee) AS current_era_fee,
        AVG(validator_fee) AS median_era_fee
    FROM
        NominatorStakesFee
    WHERE
        era_id = (SELECT last_era_id FROM LatestEra)
    GROUP BY
        nominator_id
),

TotalValidatorsPerNominator AS (
    SELECT 
        ns.nominator_id,
        COUNT(DISTINCT ns.validator_id) AS total_validators
    FROM 
        NominatorStakesFee ns
    WHERE 
        ns.era_id IN (SELECT era_id FROM Last365Eras)
    GROUP BY 
        ns.nominator_id
),

ValidatorMetrics AS (
    SELECT
        ns.nominator_id,
        COUNT(DISTINCT ns.validator_id) AS active_validators,
        tvn.total_validators
    FROM
        NominatorStakesFee ns
    JOIN
        TotalValidatorsPerNominator tvn ON ns.nominator_id = tvn.nominator_id
    WHERE
        ns.era_id = (SELECT last_era_id FROM LatestEra)
    GROUP BY
        ns.nominator_id, tvn.total_validators
),

NominatorStakeChanges AS (
    SELECT
        era_id,
        nominator_id,
        nominator_stake,
        LAG(nominator_stake, 1) OVER (PARTITION BY nominator_id ORDER BY era_id) AS previous_nominator_stake
    FROM
        NominatorStakesFee
),

NominatorStakeIncreases AS (
    SELECT
        era_id,
        nominator_id,
        nominator_stake,
        previous_nominator_stake,
        CASE 
            WHEN nominator_stake > previous_nominator_stake THEN era_id
            ELSE NULL
        END AS increase_era_id
    FROM
        NominatorStakeChanges
),

LastStakeAddition AS (
    SELECT
        nominator_id,
        MAX(increase_era_id) AS last_stake_era_id
    FROM
        NominatorStakeIncreases
    WHERE
        increase_era_id IS NOT NULL
    GROUP BY
        nominator_id
),

LastStakeAdditionEras AS (
    SELECT
        lsa.nominator_id,
        (SELECT last_era_id FROM LatestEra)+2 - lsa.last_stake_era_id AS eras_since_last_stake_addition
    FROM
        LastStakeAddition lsa
),

NominatorStats AS (
    SELECT
        n.nominator_id,        
        vm.active_validators,
        vm.total_validators,
        vm.active_validators/vm.total_validators AS coverage,
        COUNT(DISTINCT IF(n.era_id IN (SELECT era_id FROM LastKEras), n.validator_id, NULL)) AS set_change_frequency,
        COUNT(IF(n.validator_fee = 1.0 AND n.era_id = (SELECT last_era_id FROM LatestEra), 1, NULL)) AS one_hundred_percent_fee_in_set
    FROM
        NominatorStakesFee n
    LEFT JOIN
        ValidatorMetrics vm ON n.nominator_id = vm.nominator_id
    WHERE 
        n.era_id IN (SELECT era_id FROM LastKEras)
    GROUP BY
        n.nominator_id, vm.total_validators, vm.active_validators
),

Pools AS (
SELECT '13UVJyLnbVp8c4FQeiGTCUubjALfCqymb6S3BUK84jQjW6Rb' AS address
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGShx3g6i4wACj5wWbD8fHss23DBCom'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPPMjgc9jnrNGUKKf7ogtyVxHS1dyk'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNQJ1qMFCLm5m729yTi4rV6XYPNUbE'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGUNFCfqzEWK7xPkc4Fo9VL3Yv6m8Gm'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTwHCVAqkFGoro4UCHFgqzNJU1yqeX'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQVZB1mfrAxK61XPJUv1hGxJsMjRET'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMEXimERJVeonUreMF6PgH7i3277JK'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRAncAG3UJ1wox3K5sUsrEjR1CgEzL'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLoZiaZGpEcVgtAWVGYw2wSTawKySh'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEn3g17Dzc48LWUcRcxW2BxyHnPuv3'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHRQYr7QhdJL5BasaKicG8Yvuid5Eh'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFWqxtYuQC86DXwzBs2iZ4GYM4t1dr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFL9Pfgz3o76kH5eFJWfRLh9akFvN2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLZJHck3gNbB4YLiZrXXXKLbuBAW87'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNeZSoAULCnQPSqwtsjUN7CPE9Xff2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFaQpdWDBf8RNcuSAiY4vxo1GWRHJV'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHyWGWhAkpMJUxBuQ1GmfHH7CgUtPH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCw3wnW2UdtDdwoB21mxh2D3eaCByG'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKpVzjJNGnXDBWsLocTJzT32qtghfT'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCsVtunXfJuvpc8i1LtJ3LoVr8ymZi'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPkkt8KzSatMCz41YEqoAQejp5Fc4j'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCkNBRsv6NuGWSDp3dsbHXka1Fu5mw'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHnohHqFQRLK1hKZTSkiXZhiSMs8V1'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEbM6nFJeD38sFcGV4SStUPaXTn6QV'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQ8A2a3q9NvL9VnhRMsuRqoXLiVT1H'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKkw8zM4VKWt2RutpkwxcYWZvT9Wch'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGP5XSyqcpTpiV3Y5krGw1SsdgEJvJN'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGT8v3rmrZCCWpgdeTBg7wDYGVHxfWt'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNtpskyhU4oj1nfjpHkssjJEuuhAyg'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQFHk3xSiJvzTfhbP4tcBerTBbaPdq'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCovEJbQuhsZKmtH4JmFwDA7oh7dSD'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGT5MC7pYmjCBfbgCULAmZK1oZrRD8t'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGP1xbEtK2zpPKxadmzmadYMAknmieW'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLgS16efFJbqNiFcXZYEH8PXk4FSgo'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKCqRKmJS8TucfJs15PoDPnPdVHeBA'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDJT6EDsBRvCaTXru8p4xTMqBDSDab'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDVAUKKbVxyEPThBoswopQ494YP1uE'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPCfATkEPPqNoDSyiZHdmFvZX7Pkr1'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFpgFbKSKX9jzxjE68ZUSatrxGabsc'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCzdcPh9EEvb8n3by3tzo9rRh24KkA'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGH41QQPZzqGM8frBhCgVzhQ9P5NzWH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGURp4QoJ1yKT7Ui437JVsEa1qYJWts'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEUDPJLh5H2UZ5hNXMRk8fLegahSeZ'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKdoRWSSvPWDiFzzs3wFrjTe5a4kat'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJqSGt3TjLRvg9aB7xMgwxdc6r3R3s'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLNbiPt8KyaBbHUNdJ1UPbmD8rYiZD'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEiUpG9vD93oBRXASmT9eHSWNLrYUg'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGS2icXcLSp6a19RaopZo8v5uuNErFx'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGM3q9YNVx6dpKDzJQgaLYZYKGhVNXt'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHB97tJBZmH1Sqm5euhCkWT5DxTebM'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHB97tJBZmH1Sqm5euhCkWT5DxTebM'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSePBw9QHU9sZzz4cMdJuyLZ7me5sz'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRuau3hist5ugyWgr7Z6P72z4VA5fq'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTh1mXMccPFVETEgGsEHLNGSnFpPmr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMj4agrsaDhT3AWEC59ChXKRQYRsS6'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFdygNTWy88kXhrt9a3RJsKUBwxNQA'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNxPjVw1FXp4AsdBo9GEFdphqMESAt'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQ4bAq6XMuuzzQqFSWNZ3wH4RGxKkc'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGD7mKsbkoAwFSwxVvkuhYxuMXu94VW'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRy9knf2fM6Er4U8py4Sm1ZSyvhUox'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPddAeRNsesgtp97aXq6QbboyCAye2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGP96Jinvbvq3e8VXjhnHPMQ6bfrGL1'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGN6Tj8aiH1jRygEv5CBJxxUCwBfmu4'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNHAJMSddQkRSw7G1khN6g3bhWHgAW'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSxDUduwCoBXMRmJWdA4oVxsiyLckd'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMbvsCxG1HgnizbLEN8VwiGVZfMBZN'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGsJqBXeeSFMfQyqkeASrypkckm2mT'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRbkcLwBxZ4FuYjSwr2LVaQfTHTcax'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDv7fdkw25yW9K6LhfsajWcUPcqPa2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFtF7LGk6zA5A3gg4z4ppVRKsi7j3e'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGSLpzrWABD3ZpHhtfczDe9WAfywRs'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSGz3VRZag7tdVFNjEbCeYBmb8QBDt'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDfrEfwhtDxBWyGYnFrBDtWchrgDCv'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHUyQb4iV6JfEGYKZBDxe35PqAADfG'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDjR6Qu1fgxWg4Dzm7MXbo35dJDZiH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDRbcaNHiVxuENjjq2STSVXg96qsLH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQz62wQ87tzxLhAy9JxpiXA2Et4Dvr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPwTTMBunyuLgEvMUoMrJ8E8aPsfDj'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGD7kX1Mwq2uD7CfWxaJ1pjnSQtp5jQ'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEuBPV1qZY4negPWPKyCn11u8fUWZ8'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGT1nLNsEzGBrWWikVUfRBQVLeQsvHQ'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHJGqNCo8hHfm1fycchuWKW14qYJpr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKGQH4icDbUEmkGJyvu9bJJrYvpvna'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGUF7VBwNfaJTeDqi6YnSjWzd5DgUma'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJACqjZ67DNHxD4FLZnpnzrVy16xRx'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEXnF3Hzrk2oiAepWCw6WZs7c2Ens8'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPGE2ChYArqhxJQRhQnz9AT2SYw2W9'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTp9V1GEBKG9Yd9aEaEz6BKNd8uHX9'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFTH79bbcj7m4SzYD1XNB9k5RdLg3u'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMJ6aWBj5xf8wZp6L6bk4BeAxTeim6'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHYYGL22GZJzPMVmY2jK1wbrkbhmaT'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCow3AqDsqubfXBG2VNwfSH2vhSR8d'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCw4kejqSmvFyh69zCPeRFKxmaWxGm'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNDbScVKqwk6Hr9p2uC1imX8n4kVb8'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEqcXk4Xn54TVbS4QUTrQ6VSDDwA9c'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRnTBZo7Jx5FNobntQYPdHz4Dc5UDc'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDJTu6Tg9ZxEvCpqsKRkggUkJDmAD8'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHc784yL42KKYSTDWtEfPr8Kg3Eq1m'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTdSunQJpvFA5NHEJ1ivxTjyrpHLdW'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDBLBcZ4adwac2uwucR3vsRpTLgKsn'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCsV63YihAstUrqj3AGcK7gaj8eubS'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGCzcoXTLG6tYo2kczsHK4vjWa1jU3h'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSQ7kyLB9c8YwfAGgwbuQMEhS1Ubkm'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFwoy5E3tTAQK8e83qaBCPwno9fFrX'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHEhydFVMEHLbviXdmCZ8QyY9Q16jJ'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGExkFDy9M157omLxNBUZ9uYN471x4m'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJu18czmWoSFqEXd6os3KsA52Hb189'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGL4mRh7bQeYXorh8j2UiW58tXer2y2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDN2kqQyw2xa5HnHrAw74b1DDfJP9C'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGU4Quy5TKBHUAxyN9zGPboREJu4b8i'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHrNZ2nZBtLeAnH1SJG4uUEBMoQEpq'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJ358FeUYHMde39MNrn83Boa882Fm2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMAxs2H7X2eUdPuCNPb3JNbF7aZykk'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHMqh7A6vAHzv6dRbUDFtE2TzH5mDU'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRMVBP7xph2wHCuf2RzvyxJomXJ7RL'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGFeFmzaonC46ZRMx76w5va7QMMx9n'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGFeFmzaonC46ZRMx76w5va7QMMx9n'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEAP6baA9wzpmev8d5tzF8iL5Nzkhc'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTZt43T13TEpvHKnKADaaZDWwNjmny'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRitKpqoXV4vDieLuZ33FPTbJAYG14'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTG3mLgU88DB8rYYQtgph2bCLB3PLr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDV9fT5nXpwC3iQCqhL86AwDwY43Sg'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLFTzuyWm3ZXH7ZUfazmdniHHyU3K4'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGL1CZxAHdBYCemjgkAyN8AcRcDJtNr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGNmyFuCNiCiQjLFup7dqjd3FESYGN'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQJrbnukVmwKckf3MvPxZZNv737m3A'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGStectxdRLBCCLorXmeiRbSQoXoEmo'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEeuxXCcRg3U2LZiTuwoGNv3SuKEAe'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQZ82kiyddxeF6UqHLRN5BUmnoGvx2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPhC2PNgf7t23u6ZZPLSnW8GtdiUJa'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGG4wgZ8fTPB4dJZ21YasxCzie2jyC3'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDnyx9rKT9xqq9BSjxrsyhZYYjkiS7'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMMfSF92sRfU6emYJx76S6AdsuC23r'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLVjRsnjtuaquTPGb12B9Qp8yjdRnr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJxZzMx5JGSazKV55fNPhmgXwj856v'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMrCJAmV99i7MLR89n9uTLNMFRWVx3'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGojySaLryF2WL2Pmnf6V5JHhKDrTH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFD1gBnNUs6SS7AkHbVxfXeDjsBQSM'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPstbcEc1Wu1X9xuVwrVvDhfexLSyT'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQvXBCSpLRzdBcDXATTULcdZKSWjNK'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKhNHFPkhrWYsLxSquScEdz711cCiH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLBu9B2CyaZC82c2gjVRFtBpNXvjmw'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMum9uinvciSWRNa8dfFqEtpAs3w6G'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJLuQxR1TcPHRTvbH8JsviRtjKizh8'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSmWuR41rQAXtAtxa4e1fnPUxeip2m'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEHWp5Umit1V5pq2anugzwmFvG5JCG'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPKnsweqxKr37PMsgGJLX4yVMzUDtg'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLK2revpYWZrSCWveSW81hEkDR1HJV'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGR3etgMRuN1HVn8R8AUB6RgVAKbe7v'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDcHNvzQ6kwrMtK6oQLpqyz9nR8qvL'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQcgtVgHR6xyQBSHGBviT61EiEp8sP'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQoPTiYCmVyxsSJdCkSmaoadUZSLa2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDygXNiEoYyqJQ3ngXNw7R8wK4NfqE'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMndSRpBMghnCFTgAveZ5RqtKyyBr6'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGD4BfGQe3Ztsx7i4yinfSqFyVTGs8u'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSXFUTEniY9DFq5AeeccAAHdGtZQNr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGC5Q33H2KBiwUTuyFbai23eUupNDp'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGgcFxfjJ3ENCA7Vp5ePjGFMrS9FAL'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHuwQmjryMLyKsETR9mRHNkeHEwnPe'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFPiFQeHqG7RuN36EA21oFDcWBo96f'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGVugjooweDNiuF9sX8LbYfy67XAUh'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGMyL1eg6i5imfWL27VAcD9RH6Jb4X3'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEM5fpS5WM1pEunUZeR3NrHiqhcdTc'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFGaXvjgGL6mbC8CGT1K3SAgfJibef'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEQeXZPPHp29PzjvYVvPkkpBm99qYr'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRR4385GcA3GSHs71HWHMrqGgxqZzn'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLSAa8qS7SaWkNRpc9WpmWHg4J61ud'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDrYotodEcyAzE8tipNEMc61UBJAH4'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGzSYfSGDNG1yatjiMB9cnsgTdqnZH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGUBYdSz4t7J8V8tG7hH6McUA9n9K3i'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGU7ymi2m6eHoL3vp8qmjyhwhELbyjP'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGFm7PrN8Y49Qqsmn7H484gNQ2q34m1'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDYjL4GuHRyZYYednjTACJabyyvLaF'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGS6HUGZeEH6uAEP2ng59WpcNpomujQ'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSDRBkUFoD7ZUQHvkP5rGdfJfgrpCa'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJejhfBYNwQwCthqBPqdpF4DLXRYkn'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDRaoi8UkMvrtdSkrqpmiGQm26Wdse'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRHvKeAf3E2c87xD3aVac3nLr5kffS'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLs8aKWabhcpqy7xU84HQqxvWNsPZE'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTWKCJVhFzEVmCNLLJiECeh41wCW5c'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGF9SpSq4hQ67H2DJJjzcHd7kpRdozh'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTzr4E89XiH91t1vB8m3DttmPTXE2Q'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKaEZmV98vVtZB3YtCRuUpwBA8XMW2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGKD7WwtbFCPFeNovxcHTq6aKnuAMY'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDEtEVGZPxusRNaQvHJiaYqNFmtnwd'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGHffyovdqVKehXQfVjk1mkenbUnFDK'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJiJZQ8rAQRGMyfHAFLzC9agFxxjZD'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRUcts2aPd3bbNpYz91djmMjcQNwH2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJQUGhNKF5PcaYt3FypEJcxMemG9bB'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTSkLZYPUXEAc7QtMTCspkAb6Vf65P'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDYiXC36KHwXCoMepYqUU5TgrybNzB'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLcs9MhMTqbWDdJAYi2suDs4pchzme'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPSvbRZTXFrhRZGmdyK3Gt2RCsYuYj'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGSLYuENsN98DnaCpi66Z2SiEWZwNVp'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNLjA6PwQskkc24hzcCiUaa4cwqFDU'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDEu3MWNN6wum7sPtTvQJmxHNnDqPx'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGPpKjsHJE3tgN51TX6M9YKBCjWnyfV'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKPXzYdDnXUu5vBCwdurM7MnPouQgU'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGF2K6xvT8U5SxrJQM2yuXp4pyYZLxf'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGK28r6uP5jSv9QSX4Wsk5gCzsAfVMC'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGRYBkbytB63vkTmzxzWz7ftCXqvEGF'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGM7Q1HKojZe9UJwkPY5gvU4nC92jAo'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNazb4DAYjn5EMtVv2E7zCfvJhzLK2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTPBUpb5h4DqT2TSNbhXSqe8B482NH'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGN2tsPdQVYj6pbHU6Lfxb3wk1k8WK2'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGE3FP7fYb1zATV1EfNtHVKfQEVv9q9'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNqG222PgboPrhiHqSFXVpmmzU9fXh'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGD4CU8eT1hvvHs13wuQMB4NtcTbcvb'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGLjzrqby2mcAXoD4WR3af2uzfVngja'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQrxKTVWYxzJ2XG5Bbx7xi76PzyPMC'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGREMTuDMFm2Gy2zm4izEE9FsveDbya'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJDmhUWPtgNd7J1hKRJBAuNxtSeQko'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGZUYUm7j7DhszCbrNdgyTCS1Z4HZw'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQNRTXs4HEwemqcVLmuJwTuP2Uekwo'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGE6pErcrNUzVcZxgeEPdsEBs9wTPrW'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGTsiLkDXxnGUhi72DRkLU5qqYaSb2G'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGUJgLvtgT3JnoJoA5QHo7RX5zfDida'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGQjpbyatz2ydiMMBDtwRCu4AZ7u2vx'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDBKNkKFcVuYGHcxwRoNCeJuLLMQys'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGd3QDiRWaE335A3qE93MMitvzbkJk'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGG1NppBMfvAjUDba2h5XaJUFibCWcS'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGGvsgvUxRuFgpVwHjVfoEtMDYCJKrD'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGDN1wyBAxtvXjYVJszKRLMtJ6eyaFo'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNXRjKFrmGmk5Gw3wAimcJ9TPGSykN'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGS9rL1Wx1k7EKKLUmXaVtj8qkFKNPn'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGH7aG9LsnJGgHkodg4BrNbvcJWvSZ8'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGL8LHS4uC7Yrxweahsz4syfMT6PVDW'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGG8WYJ5yErBPnPWTzQ6EL7XBZUHK4H'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGJHLZDThg9NxGNy9JGoXYouRotBSZw'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGKWfi2XqMTVZQ666uLvZ6vQiEgzBdV'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGEDwxLXTwR19vjsabwQLd3EnzpY5ze'
UNION ALL
SELECT '13UVJyLnbVp8c4FQeiGNi8JY7n7fnjYXoPsjEpk1ir9b4xBh'
),

 ActiveNominators AS (
                SELECT
                    n.nominator_id,
                    (SELECT last_era_id FROM LatestEra) AS lastActiveEra,
                    FORMAT('%.2f', CAST(n.current_stake AS NUMERIC)) AS currentStake,
                    FORMAT('%.2f', CAST(n.last_era_reward AS NUMERIC)) AS lastEraReward,

                    FORMAT('%.2f', CAST(yr.yearly_total_reward/yr.average_stake AS NUMERIC)*100) AS rewardYear,
                    FORMAT('%.2f', CAST((POWER(1 + (yr.yearly_total_reward / yr.average_stake) / 365, 365) - 1) AS NUMERIC)*100) AS APY,

                    lsae.eras_since_last_stake_addition AS lastStakeAddition,

                    ns.total_validators AS totalValidators,
                    ns.active_validators AS activeValidators,

                    nvm.active_count AS wasActiveCount,
                    nvm.was_active AS wasActiveInMonth,
                    FORMAT('%.2f', nvm.success_rate * 100) AS activeEraSuccessRate,
                    FORMAT('%.2f', CAST(cef.median_era_fee * 100 AS NUMERIC)) AS medianErasFee,
                    FORMAT('%.2f', CAST(cef.current_era_fee * 100 AS NUMERIC)) AS currentEraFee,
                    FORMAT('%.2f', CAST(ns.coverage * 100 AS NUMERIC)) AS validatorsCoverage,
                    ns.one_hundred_percent_fee_in_set AS validatorsMaxFeeInSet,  -- 100% Fee
                    ns.set_change_frequency AS validatorsSetChange,
                    FORMAT('%.2f', CAST(hms.highest_min_stake AS NUMERIC)) AS highestMinStake,
                	CASE
                    WHEN ntile(5) OVER (ORDER BY n.last_era_reward) = 1 THEN 'low activity'
                    WHEN ntile(5) OVER (ORDER BY n.last_era_reward) = 2 THEN 'moderate'
                    WHEN ntile(5) OVER (ORDER BY n.last_era_reward) = 3 THEN 'unstable'
                    WHEN ntile(5) OVER (ORDER BY n.last_era_reward) = 4 THEN 'stable'
                    WHEN ntile(5) OVER (ORDER BY n.last_era_reward) = 5 THEN 'top_performer'
                    ELSE 'Unknown'
                END AS tags,
		            CASE
                    WHEN rq.reward_quantile = 1 THEN 'low stake'
                    WHEN rq.reward_quantile = 2 THEN 'inactive'
                    WHEN rq.reward_quantile = 3 THEN 'unstable'
                    WHEN rq.reward_quantile = 4 THEN 'stable'
                    WHEN rq.reward_quantile = 5 THEN 'top_performer'
                    ELSE 'Unknown'
                END AS tagsOld,
                CASE
                    WHEN ntile(4) OVER (ORDER BY n.current_stake) = 1 THEN 'low stake'
                    WHEN ntile(4) OVER (ORDER BY n.current_stake) = 2 THEN 'low prob'
                    WHEN ntile(4) OVER (ORDER BY n.current_stake) = 3 THEN 'in risk'
                    WHEN ntile(4) OVER (ORDER BY n.current_stake) = 4 THEN 'low risk'
                    ELSE 'Unknown'
                END AS risks,
                CASE
                    WHEN sq.stake_quantile = 1 THEN 'low stake'
                    WHEN sq.stake_quantile = 2 THEN 'low prob'
                    WHEN sq.stake_quantile = 3 THEN 'in risk'
                    WHEN sq.stake_quantile = 4 THEN 'low risk'
                    ELSE 'Unknown'
                END AS risksOld,
		CASE
		    WHEN n.nominator_id IN ( SELECT address FROM Pools ) THEN 'True'
		    ELSE 'False'
		END AS isPool
            FROM
                LatestEraStats n
            LEFT JOIN
                YearlyReward yr ON n.nominator_id = yr.nominator_id
            LEFT JOIN
                NominatorStats ns ON n.nominator_id = ns.nominator_id
            LEFT JOIN
                LastStakeAdditionEras lsae ON n.nominator_id = lsae.nominator_id
            LEFT JOIN
                NominatorValidatorsMetrics nvm ON n.nominator_id = nvm.nominator_id
            LEFT JOIN
                CurrentEraFee cef ON n.nominator_id = cef.nominator_id
            LEFT JOIN
                HighestMinStake hms ON ns.nominator_id = hms.nominator_id
            LEFT JOIN
                RewardQuantiles rq ON n.nominator_id = rq.nominator_id
            LEFT JOIN
                StakeQuantiles sq ON n.nominator_id = sq.nominator_id
            ORDER BY ns.nominator_id, n.current_stake DESC
),


AllNominators AS (
    SELECT DISTINCT account_id AS nominator_id
    FROM data.polkadot__nominators
),

LatestEraNominators AS (
    SELECT DISTINCT account_id AS nominator_id
    FROM data.polkadot__stake_nominators
    WHERE era_id = (SELECT last_era_id FROM LatestEra)
),

InactiveNominators AS (
    SELECT nominator_id
    FROM AllNominators
    WHERE nominator_id NOT IN (SELECT nominator_id FROM LatestEraNominators)
),

LastActiveNominatorStake AS (
    SELECT
        sn.account_id AS nominator_id,
        MAX(sn.value) / 1e10 AS max_nominator_stake
    FROM data.polkadot__stake_nominators sn
    INNER JOIN (
        SELECT account_id, MAX(era_id) AS max_era
        FROM data.polkadot__stake_nominators
        GROUP BY account_id
    ) max_sn ON sn.account_id = max_sn.account_id AND sn.era_id = max_sn.max_era
    WHERE sn.account_id IN (SELECT nominator_id FROM InactiveNominators)
    GROUP BY sn.account_id
),

FinalInactiveNominators AS (
                    SELECT DISTINCT
                        ins.nominator_id,
                        ins.max_nominator_stake AS last_active_stake,
                        CASE
                            WHEN ins.max_nominator_stake < 572 THEN 'low stake'
                            ELSE 'poor validators set'
                        END AS tags
                    FROM LastActiveNominatorStake ins
                        )


SELECT Distinct 
    an.nominator_id AS address,
    an.lastEraReward,
    an.APY AS rewardYear,
    an.APY,
    an.currentStake,
    an.lastStakeAddition,
    an.totalValidators,
    an.activeValidators,
    an.wasActiveInMonth,
    an.lastActiveEra,
    an.activeEraSuccessRate,
    an.medianErasFee,
    an.currentEraFee,
    an.validatorsCoverage,
    an.validatorsSetChange,
    an.highestMinStake,
    an.tags,
    -- an.tagsOld,
    an.risks,
    -- an.risksOld,
    1 as activeNominator,
    an.isPool
FROM
    ActiveNominators an

UNION ALL

SELECT
    fina.nominator_id AS address,
    NULL AS lastEraReward,
    NULL AS rewardYear,
    NULL AS APY,
    CAST(fina.last_active_stake AS STRING) AS currentStake,
    NULL AS lastStakeAddition,
    NULL AS totalValidators,
    NULL AS activeValidators,
    NULL AS wasActiveInMonth,
    NULL AS lastActiveEra,
    NULL AS activeEraSuccessRate,
    NULL AS medianErasFee,
    NULL AS currentEraFee,
    NULL AS validatorsCoverage,
    NULL AS validatorsSetChange,
    NULL AS highestMinStake,
    fina.tags AS tags,
    NULL AS risks,
    0 AS activeNominator,
    'INACTIVE' AS isPool
FROM
    FinalInactiveNominators fina

