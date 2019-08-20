from test.integration.base import DBTIntegrationTest, use_profile


class TestQueryHeader(DBTIntegrationTest):
    @property
    def project_config(self):
        return {
            "query_header": "-- debug {{ get_unique_id() }}"
        }

    @property
    def schema(self):
        return "query_header_049"

    @staticmethod
    def dir(path):
        return path.lstrip('/')

    @property
    def models(self):
        return self.dir("models")

    @use_profile("postgres")
    def test__postgres__simple_copy(self):
        results = self.run_dbt(["run"])

        self.assertEquals(len(results), 3)

        for result in results:
            path = result.node.get('build_path').replace("compiled", "run")
            with open(path, 'r') as f:
                run_contents = f.read()
                self.assertIn('-- debug {}'.format(result.node.get('unique_id')), run_contents)
