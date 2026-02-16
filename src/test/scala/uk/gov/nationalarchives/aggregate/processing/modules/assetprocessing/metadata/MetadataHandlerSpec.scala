package uk.gov.nationalarchives.aggregate.processing.modules.assetprocessing.metadata

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.gov.nationalarchives.aggregate.processing.ExternalServiceSpec

class MetadataHandlerSpec extends ExternalServiceSpec {
  "'base property' case objects ids" should "match their schema defined metadata property name" in {
    ClosurePeriodProperty.id shouldBe "closure_period"
    ClosureTypeProperty.id shouldBe "closure_type"
    DescriptionClosedProperty.id shouldBe "description_closed"
    ClientSideChecksumProperty.id shouldBe "client_side_checksum"
    ClosureStartDateProperty.id shouldBe "closure_start_date"
    DateLastModifiedProperty.id shouldBe "date_last_modified"
    EndDateProperty.id shouldBe "end_date"
    FileNameProperty.id shouldBe "file_name"
    FilePathProperty.id shouldBe "file_path"
    FileSizeProperty.id shouldBe "file_size"
    FoiExemptionCodeProperty.id shouldBe "foi_exemption_code"
    FoiScheduleDateProperty.id shouldBe "foi_schedule_date"
    MatchIdProperty.id shouldBe "matchId"
    TransferIdProperty.id shouldBe "transferId"
    TitleClosedProperty.id shouldBe "title_closed"
  }
}
