from sqlalchemy.orm import Session

from interfaces.i_orm_transformer import IORMTransformer
from sources.cordis.object_transformer import (
    CordisProject,
)


class OpenaireORMTransformer(IORMTransformer):

    def __init__(self, session: Session):
        super().__init__(session)

    def map_to_orm(self, cordis_project: CordisProject):
        pass
        # self.session.flush()  # Get all serial IDs for the instances # updates ID's
