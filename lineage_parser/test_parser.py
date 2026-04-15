import unittest
from parser_api import parse_sql_lineage


class TestParserLineage(unittest.TestCase):
    def test_simple_select(self):
        sql = "SELECT * FROM users"
        out = parse_sql_lineage(sql)
        self.assertEqual(out["writes_tables"], [])
        self.assertIn("public.users", out["reads_tables"])

    def test_join_select(self):
        sql = """
        SELECT u.id, o.id
        FROM public.users u
        JOIN public.orders o ON o.user_id = u.id
        """
        out = parse_sql_lineage(sql)
        self.assertIn("public.users", out["reads_tables"])
        self.assertIn("public.orders", out["reads_tables"])
        self.assertEqual(out["writes_tables"], [])

    def test_insert_select(self):
        sql = """
        INSERT INTO analytics.user_orders
        SELECT u.id, o.id
        FROM public.users u
        JOIN public.orders o ON o.user_id = u.id
        """
        out = parse_sql_lineage(sql)
        self.assertIn("analytics.user_orders", out["writes_tables"])
        self.assertIn("public.users", out["reads_tables"])
        self.assertIn("public.orders", out["reads_tables"])
        self.assertNotIn("analytics.user_orders", out["reads_tables"])

    def test_create_table_as_select(self):
        sql = """
        CREATE TABLE analytics.daily_sales AS
        SELECT * FROM public.orders
        """
        out = parse_sql_lineage(sql)
        self.assertIn("analytics.daily_sales", out["writes_tables"])
        self.assertIn("public.orders", out["reads_tables"])

    def test_update(self):
        sql = "UPDATE public.users SET email='x' WHERE id=1"
        out = parse_sql_lineage(sql)
        self.assertIn("public.users", out["writes_tables"])

    def test_merge_into(self):
        sql = """
        MERGE INTO analytics.customer_dim t
        USING staging.customers s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET email=s.email
        WHEN NOT MATCHED THEN INSERT (id,email) VALUES (s.id,s.email);
        """
        out = parse_sql_lineage(sql)
        self.assertIn("analytics.customer_dim", out["writes_tables"])
        self.assertIn("staging.customers", out["reads_tables"])

    def test_cte_not_counted_as_table(self):
        sql = """
        WITH recent_orders AS (
            SELECT * FROM public.orders
        )
        SELECT * FROM recent_orders
        """
        out = parse_sql_lineage(sql)
        self.assertIn("public.orders", out["reads_tables"])
        self.assertNotIn("public.recent_orders", out["reads_tables"])
        self.assertNotIn("recent_orders", out["reads_tables"])

    def test_quoted_names(self):
        sql = 'SELECT * FROM "public"."users"'
        out = parse_sql_lineage(sql)
        self.assertIn("public.users", out["reads_tables"])

    def test_three_part_name(self):
        sql = "SELECT * FROM prod.public.users"
        out = parse_sql_lineage(sql)
        self.assertIn("public.users", out["reads_tables"])


if __name__ == "__main__":
    unittest.main()
