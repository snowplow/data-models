CREATE OR REPLACE TABLE {{.scratch_schema}}.sessions_lasts{{.entropy}}
AS(
  SELECT
    a.domain_sessionid,
    a.page_title AS last_page_title,

    a.page_url AS last_page_url,

    a.page_urlscheme AS last_page_urlscheme,
    a.page_urlhost AS last_page_urlhost,
    a.page_urlpath AS last_page_urlpath,
    a.page_urlquery AS last_page_urlquery,
    a.page_urlfragment AS last_page_urlfragment

  FROM
    {{.scratch_schema}}.page_views_staged{{.entropy}} a

  INNER JOIN {{.scratch_schema}}.sessions_aggregates{{.entropy}} b
  ON a.domain_sessionid = b.domain_sessionid
  -- Don't join on timestamp because people can return to a page after previous page view is complete.
  AND a.page_view_in_session_index = b.page_views
)
