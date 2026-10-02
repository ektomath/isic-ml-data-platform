import hashlib
import zipfile
from pathlib import Path

from data_platform.files import (
    IMAGE_SUFFIXES,
    ArchiveMatches,
    check_archives_exist,
    extract_zip_members,
    format_bronze_uri,
    is_image_file,
    iter_archive_image_rows,
    parse_bronze_uri,
    stage_archive_locally,
    stage_archives_and_extract_metadata,
)


def test_extract_zip_members_extracts_only_matching_members(tmp_path):
    archive_path = tmp_path / "archive.zip"
    destination_root = tmp_path / "out"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", "image-a")
        archive.writestr("metadata.csv", "image,dx\nISIC_1,nv\n")
        archive.writestr("licenses/license.txt", "license")

    extracted_paths = extract_zip_members(
        archive_path,
        destination_root,
        lambda path: path.suffix == ".jpg" or path.parts[0] == "licenses",
    )

    assert extracted_paths == [
        destination_root / "a.jpg",
        destination_root / "licenses" / "license.txt",
    ]
    assert (destination_root / "a.jpg").read_text() == "image-a"
    assert (destination_root / "licenses" / "license.txt").read_text() == "license"
    assert not (destination_root / "metadata.csv").exists()


def test_stage_archive_locally_copies_archive_once_and_reuses_complete_copy(tmp_path):
    archive_path = tmp_path / "archive.zip"
    local_root = tmp_path / "stage"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", "image-a")

    staged_path = stage_archive_locally(archive_path, local_root)
    reused_path = stage_archive_locally(archive_path, local_root)

    assert staged_path == local_root / "archive.zip"
    assert reused_path == staged_path
    assert staged_path.read_bytes() == archive_path.read_bytes()


def test_stage_archive_locally_rejects_mismatched_existing_copy(tmp_path):
    archive_path = tmp_path / "archive.zip"
    local_root = tmp_path / "stage"
    local_root.mkdir()
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", "image-a")
    (local_root / "archive.zip").write_text("partial")

    try:
        stage_archive_locally(archive_path, local_root)
    except RuntimeError as error:
        assert "differs from source" in str(error)
    else:
        raise AssertionError("Expected mismatched staged archive to fail")


def test_iter_archive_image_rows_reads_image_bytes_without_extracting(tmp_path):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("images/a.jpg", b"image-a")
        archive.writestr("b.PNG", b"image-b")
        archive.writestr("metadata.csv", "image,dx\nISIC_1,nv\n")

    rows = list(
        iter_archive_image_rows(
            archive_path,
            source_split="train",
            source_archive_uri="dbfs:/landing/archive.zip",
        )
    )

    assert rows == [
        {
            "image_id": "a",
            "source_split": "train",
            "archive_member_path": "images/a.jpg",
            "source_archive_uri": "dbfs:/landing/archive.zip",
            "image_bytes": b"image-a",
            "byte_length": 7,
            "source_checksum": "84127d9feb9345703f2ea1ce0c14f6dfb935b8b04816230d160f03922c94ff31",
        },
        {
            "image_id": "b",
            "source_split": "train",
            "archive_member_path": "b.PNG",
            "source_archive_uri": "dbfs:/landing/archive.zip",
            "image_bytes": b"image-b",
            "byte_length": 7,
            "source_checksum": "657f504b469e7f2a0d8ce3cd481194445f99ee57b40fc9d7fe28d8ecad1fc09b",
        },
    ]
    assert not (tmp_path / "images").exists()


def test_iter_archive_image_rows_applies_member_predicate_before_reading_bytes(tmp_path):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", b"image-a")
        archive.writestr("b.jpg", b"image-b")

    rows = list(
        iter_archive_image_rows(
            archive_path,
            source_split="train",
            member_predicate=lambda path: path.name == "a.jpg",
        )
    )

    assert [row["image_id"] for row in rows] == ["a"]


def test_is_image_file_uses_known_image_suffixes():
    assert {".jpg", ".jpeg", ".png"}.issubset(IMAGE_SUFFIXES)
    assert is_image_file(Path("ISIC_1.JPG"))
    assert is_image_file(Path("ISIC_2.jpeg"))
    assert not is_image_file(Path("metadata.csv"))


def test_check_archives_exist_passes_when_all_archives_present(tmp_path, caplog):
    archive_path = tmp_path / "archive.zip"
    archive_path.write_bytes(b"stub")

    with caplog.at_level("INFO", logger="data_platform"):
        check_archives_exist([{"archive_local_path": archive_path, "archive_dbfs_path": "landing/archive.zip"}])

    assert "present" in caplog.text


def test_check_archives_exist_raises_on_missing_archive(tmp_path):
    missing_path = tmp_path / "missing.zip"

    try:
        check_archives_exist([{"archive_local_path": missing_path, "archive_dbfs_path": "landing/missing.zip"}])
    except FileNotFoundError as error:
        assert "missing.zip" in str(error)
    else:
        raise AssertionError("Expected missing archive to fail")


def _build_archive_dict(tmp_path, metadata_members: dict[str, str], metadata_filename: str = "metadata.csv") -> dict:
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", "image-a")
        for name, content in metadata_members.items():
            archive.writestr(name, content)
    return {
        "archive_local_path": archive_path,
        "archive_filename": "archive.zip",
        "metadata_filename": metadata_filename,
        "metadata_local_root": tmp_path / "metadata_out",
    }


def test_stage_archives_and_extract_metadata_sets_paths_on_archive_dict(tmp_path):
    archive = _build_archive_dict(tmp_path, {"metadata.csv": "image,dx\nISIC_1,nv\n"})
    local_stage_root = tmp_path / "stage"

    result = stage_archives_and_extract_metadata([archive], local_stage_root)

    assert result[0]["staged_archive_path"] == local_stage_root / "archive.zip"
    assert result[0]["metadata_target_path"] == str(tmp_path / "metadata_out" / "metadata.csv")
    assert Path(result[0]["metadata_target_path"]).read_text() == "image,dx\nISIC_1,nv\n"


def test_stage_archives_and_extract_metadata_rejects_missing_metadata_file(tmp_path):
    archive = _build_archive_dict(tmp_path, {})

    try:
        stage_archives_and_extract_metadata([archive], tmp_path / "stage")
    except RuntimeError as error:
        assert "Expected one metadata.csv" in str(error)
    else:
        raise AssertionError("Expected missing metadata file to fail")


def test_stage_archives_and_extract_metadata_rejects_duplicate_metadata_files(tmp_path):
    archive = _build_archive_dict(
        tmp_path,
        {"metadata.csv": "a", "nested/metadata.csv": "b"},
    )

    try:
        stage_archives_and_extract_metadata([archive], tmp_path / "stage")
    except RuntimeError as error:
        assert "extracted 2" in str(error)
    else:
        raise AssertionError("Expected duplicate metadata files to fail")


def test_parse_bronze_uri_recovers_archive_uri_and_member_path():
    source_archive_uri, archive_member_path = parse_bronze_uri(
        "archive:dbfs:/landing/archive.zip#images/a.jpg"
    )

    assert source_archive_uri == "dbfs:/landing/archive.zip"
    assert archive_member_path == "images/a.jpg"


def test_format_bronze_uri_round_trips_with_parse_bronze_uri():
    uri = format_bronze_uri("dbfs:/landing/archive.zip", "images/a.jpg")

    assert uri == "archive:dbfs:/landing/archive.zip#images/a.jpg"
    assert parse_bronze_uri(uri) == ("dbfs:/landing/archive.zip", "images/a.jpg")


def _checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_archive_matches_yields_verified_matches_across_archives(tmp_path):
    image_a, image_b = b"image-a", b"image-b"
    archive_1 = tmp_path / "archive1.zip"
    archive_2 = tmp_path / "archive2.zip"
    with zipfile.ZipFile(archive_1, "w") as archive:
        archive.writestr("a.jpg", image_a)
    with zipfile.ZipFile(archive_2, "w") as archive:
        archive.writestr("b.jpg", image_b)

    matches = ArchiveMatches(
        {
            "uri1": {"a.jpg": {"image_id": "a", "source_checksum": _checksum(image_a)}},
            "uri2": {"b.jpg": {"image_id": "b", "source_checksum": _checksum(image_b)}},
        },
        {"uri1": archive_1, "uri2": archive_2},
    )

    assert {candidate["image_id"] for candidate, _archive_row in matches} == {"a", "b"}
    assert matches.checksum_mismatches == []
    assert matches.missing == []


def test_archive_matches_collects_checksum_mismatch_without_raising(tmp_path):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", b"image-a")
    candidate = {"image_id": "a", "source_checksum": _checksum(b"different-bytes")}

    matches = ArchiveMatches({"uri1": {"a.jpg": candidate}}, {"uri1": archive_path})

    assert list(matches) == []
    assert len(matches.checksum_mismatches) == 1
    assert matches.checksum_mismatches[0]["candidate"] == candidate
    assert matches.missing == []


def test_archive_matches_collects_missing_candidate_for_unstaged_archive():
    candidate = {"image_id": "a", "source_checksum": _checksum(b"whatever")}

    matches = ArchiveMatches({"uri1": {"a.jpg": candidate}}, {})

    assert list(matches) == []
    assert matches.checksum_mismatches == []
    assert matches.missing == [candidate]


def test_parse_bronze_uri_rejects_non_archive_uri():
    try:
        parse_bronze_uri("table:bronze.x_image_blobs/train/a")
    except ValueError as error:
        assert "Not a resolvable archive bronze_uri" in str(error)
    else:
        raise AssertionError("Expected non-archive bronze_uri to fail")
