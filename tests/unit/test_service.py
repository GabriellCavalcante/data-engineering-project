from company_name.project_name.service.service import Service


def test_service_calls_components_in_expected_order():
    calls = []

    class LoadControl:
        def can_start(self, context):
            calls.append("load_control.can_start")
            return True

        def start(self, context):
            calls.append("load_control.start")
            return "run-1"

        def mark_success(self, run_id, context):
            calls.append("load_control.mark_success")

        def mark_failure(self, run_id, error, context):
            calls.append("load_control.mark_failure")

    class Reader:
        def read(self, path):
            calls.append("reader.read")
            return "input-dataframe"

    class Validator:
        def validate(self, dataframe):
            calls.append("validator.validate")

    class Processor:
        def process(self, dataframe):
            calls.append("processor.process")
            return "output-dataframe"

    class Writer:
        def write(self, dataframe, path):
            calls.append("writer.write")

    service = Service(
        reader=Reader(),
        writer=Writer(),
        validator=Validator(),
        processor=Processor(),
        load_control=LoadControl(),
    )

    service.run("s3://input", "s3://output")

    assert calls == [
        "load_control.can_start",
        "load_control.start",
        "reader.read",
        "validator.validate",
        "processor.process",
        "writer.write",
        "load_control.mark_success",
    ]
