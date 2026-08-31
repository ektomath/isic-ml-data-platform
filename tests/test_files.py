import zipfile
from pathlib import Path

from data_platform.files import (
    IMAGE_SUFFIXES,
    check_archives_exist,
    count_zip_members_by_suffix,
    extract_zip_members,
    is_image_file,
    iter_image_blob_rows,
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


def test_iter_image_blob_rows_reads_image_bytes_without_extracting(tmp_path):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("images/a.jpg", b"image-a")
        archive.writestr("b.PNG", b"image-b")
        archive.writestr("metadata.csv", "image,dx\nISIC_1,nv\n")

    rows = list(
        iter_image_blob_rows(
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


def test_count_zip_members_by_suffix_groups_by_lowercase_suffix(tmp_path):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("a.jpg", "image-a")
        archive.writestr("b.JPG", "image-b")
        archive.writestr("metadata.csv", "image,dx\nISIC_1,nv\n")
        archive.writestr("dir/", "")

    assert count_zip_members_by_suffix(archive_path) == {".jpg": 2, ".csv": 1}


def test_count_zip_members_by_suffix_empty_archive_returns_empty_dict(tmp_path):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w"):
        pass

    assert count_zip_members_by_suffix(archive_path) == {}


def test_is_image_file_uses_known_image_suffixes():
    assert {".jpg", ".jpeg", ".png"}.issubset(IMAGE_SUFFIXES)
    assert is_image_file(Path("ISIC_1.JPG"))
    assert is_image_file(Path("ISIC_2.jpeg"))
    assert not is_image_file(Path("metadata.csv"))


def test_check_archives_exist_passes_when_all_archives_present(tmp_path, capsys):
    archive_path = tmp_path / "archive.zip"
    archive_path.write_bytes(b"stub")

    check_archives_exist([{"archive_local_path": archive_path, "archive_dbfs_path": "landing/archive.zip"}])

    assert "present" in capsys.readouterr().out


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
