from lib.database.create_db_session import create_db_session


class BatchRequester:

    def __init__(self, model, batch_size=64):
        self.model = model
        self.batch_size = batch_size
        self.session_factory = create_db_session()

    def next_batch(self, offset_start: int = 0):
        with self.session_factory() as session:
            offset = offset_start
            while True:
                batch = session.query(self.model.id, self.model.full_text) \
                    .filter(self.model.full_text.isnot(None)) \
                    .order_by(self.model.id) \
                    .offset(offset) \
                    .limit(self.batch_size) \
                    .all()

                if not batch:
                    break

                yield batch
                offset += self.batch_size
