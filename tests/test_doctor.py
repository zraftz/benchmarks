import unittest

from benchctl.doctor import assess


def observed(**versions):
    return {
        name: {"path": f"/tools/{name}", "version": version, "returncode": 0}
        for name, version in versions.items()
    }


class DoctorTests(unittest.TestCase):
    def required(self):
        return {"rustc": "1.88.0", "cargo": "1.88.0", "go": (1, 22), "python3": (3, 11)}

    def passing(self):
        return observed(
            rustc="rustc 1.88.0 (hash date)",
            cargo="cargo 1.88.0 (hash date)",
            go="go version go1.24.1 linux/amd64",
            python3="Python 3.14.6",
            protoc="libprotoc 29.3",
            git="git version 2.54.0",
            cc="cc (GCC) 14.2.0",
        )

    def test_exact_rust_and_minimum_python_go_pass(self):
        result = assess(self.passing(), self.required())
        self.assertEqual(result["status"], "passed")
        self.assertTrue(all(tool["status"] == "passed" for tool in result["tools"].values()))

    def test_wrong_or_missing_required_tool_fails_closed(self):
        for name, value in (
            ("rustc", "rustc 1.89.0 (hash date)"),
            ("cargo", "cargo 1.87.0 (hash date)"),
            ("go", "go version go1.21.13 linux/amd64"),
            ("python3", "Python 3.10.14"),
            ("protoc", None),
            ("git", None),
            ("cc", None),
        ):
            with self.subTest(name=name):
                tools = self.passing()
                if value is None:
                    tools[name] = {"path": None, "version": None, "returncode": None}
                else:
                    tools[name]["version"] = value
                result = assess(tools, self.required())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["tools"][name]["status"], "failed")

    def test_unparseable_version_fails_a_versioned_tool(self):
        tools = self.passing()
        tools["go"]["version"] = "unknown"
        self.assertEqual(assess(tools, self.required())["tools"]["go"]["status"], "failed")
