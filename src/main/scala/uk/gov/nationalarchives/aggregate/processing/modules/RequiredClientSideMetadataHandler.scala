package uk.gov.nationalarchives.aggregate.processing.modules

import graphql.codegen.types.ClientSideMetadataInput
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource
import uk.gov.nationalarchives.aggregate.processing.modules.Common.AssetSource.AssetSource

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

object RequiredClientSideMetadataHandler {
  implicit class StringTimeConversions(sc: StringContext) {
    def t(args: Any*): Timestamp =
      Timestamp.from(Instant.parse(sc.s(args: _*)))
  }

  implicit val sharePointDecoder: Decoder[SharePointRequiredMetadata] = deriveDecoder[SharePointRequiredMetadata]

  trait RequiredClientSideMetadata {
    def matchId: String
  }

  case class SharePointRequiredMetadata(matchId: String, transferId: UUID, Modified: String, SHA256ClientSideChecksum: String, Length: Long, FileRef: String, FileLeafRef: String)
      extends RequiredClientSideMetadata

  private case class SharePointLocationPath(root: String, site: String, library: String, filePath: String)

  private def sharePointLocationPathToFilePath(locationPath: String): SharePointLocationPath = {
    val pathComponents = locationPath.split("/")
    SharePointLocationPath(pathComponents(1), pathComponents(2), pathComponents(3), pathComponents.slice(1, pathComponents.length).mkString("/"))
  }

  def getRequiredMetadataDecoder(assetSource: AssetSource): Decoder[_ >: SharePointRequiredMetadata <: RequiredClientSideMetadata] = {
    assetSource match {
      case AssetSource.SharePoint => RequiredClientSideMetadataHandler.sharePointDecoder
    }
  }

  def toClientSideMetadataInput[T <: RequiredClientSideMetadata](metadata: T): ClientSideMetadataInput = {
    metadata match {
      case SharePointRequiredMetadata(matchId, _, modified, sha256ClientSideChecksum, length, fileRef, _) =>
        val dateLastModified = t"$modified".getTime
        val sharePointLocation = sharePointLocationPathToFilePath(fileRef)
        ClientSideMetadataInput(sharePointLocation.filePath, sha256ClientSideChecksum, dateLastModified, length, matchId)
    }
  }
}
