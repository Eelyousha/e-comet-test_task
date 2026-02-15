WITH 
    -- Получаем последнее значение views для каждого часа по каждой фразе
    hourly_views AS (
        SELECT 
            phrase,
            toHour(dt) AS hour,
            argMax(views, dt) AS max_views  -- Берём views с максимальным dt в пределах часа
        FROM phrases_views
        WHERE campaign_id = 1111111
          AND toDate(dt) = today()  -- Только за сегодня
        GROUP BY phrase, hour
    ),
    -- Вычисляем прирост просмотров за каждый час
    views_diff AS (
        SELECT 
            phrase,
            hour,
            max_views - lagInFrame(max_views, 1, 0) OVER (PARTITION BY phrase ORDER BY hour) AS views_in_hour
        FROM hourly_views
    )
-- Формируем итоговый результат с массивом (hour, views)
SELECT 
    phrase,
    groupArray((hour, views_in_hour)) AS views_by_hour
FROM views_diff
WHERE views_in_hour > 0  -- Только часы с просмотрами
GROUP BY phrase
ORDER BY phrase;
