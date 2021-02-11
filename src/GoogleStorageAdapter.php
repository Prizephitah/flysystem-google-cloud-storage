<?php
declare(strict_types=1);

namespace Superbalist\Flysystem\GoogleStorage;


use DateTime;
use Exception;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Storage\Acl;
use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use GuzzleHttp\Psr7\StreamWrapper;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\DirectoryListing;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\InvalidVisibilityProvided;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckFileExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use Throwable;

class GoogleStorageAdapter implements FilesystemAdapter {
	/**
	 * @const STORAGE_API_URI_DEFAULT
	 */
	public const STORAGE_API_URI_DEFAULT = 'https://storage.googleapis.com';

	/**
	 * @var StorageClient
	 */
	protected StorageClient $storageClient;

	/**
	 * @var Bucket
	 */
	protected Bucket $bucket;

	/**
	 * @var string
	 */
	protected string $storageApiUri;

	/**
	 * @var PathPrefixer
	 */
	protected PathPrefixer $prefixer;

	/**
	 * @param StorageClient $storageClient
	 * @param Bucket $bucket
	 * @param string $pathPrefix
	 * @param string $storageApiUri
	 */
	public function __construct(StorageClient $storageClient, Bucket $bucket, $pathPrefix = null, $storageApiUri = null)
	{
		$this->storageClient = $storageClient;
		$this->bucket = $bucket;

		if ($pathPrefix) {
			$this->prefixer = new PathPrefixer($pathPrefix, DIRECTORY_SEPARATOR);
		}

		$this->storageApiUri = ($storageApiUri) ?: self::STORAGE_API_URI_DEFAULT;
	}

	/**
	 * @param string $path
	 * @throws FilesystemException
	 * @return bool
	 */
	public function fileExists(string $path): bool {
		try {
			return $this->getObject($path)->exists();
		} catch (Throwable $t) {
			throw UnableToCheckFileExistence::forLocation($path, $t);
		}
	}

	/**
	 * @param string $path
	 * @param string $contents
	 * @param Config $config
	 *
	 * @throws FilesystemException
	 */
	public function write(string $path, string $contents, Config $config): void {
		try {
			$this->upload($path, $contents, $config);
		} catch (Throwable $t) {
			throw UnableToWriteFile::atLocation($path, 'Error while uploading to bucket: '.$t->getMessage(), $t);
		}
	}

	/**
	 * @param string $path
	 * @param resource $contents
	 * @param Config $config
	 *
	 * @throws UnableToWriteFile
	 * @throws FilesystemException
	 */
	public function writeStream(string $path, $contents, Config $config): void {
		try {
			$this->upload($path, $contents, $config);
		} catch (Throwable $t) {
			throw UnableToWriteFile::atLocation($path, 'Error while uploading to bucket: '.$t->getMessage(), $t);
		}
	}

	/**
	 * Uploads a file to the Google Cloud Storage service.
	 *
	 * @param string $path
	 * @param string|resource $contents
	 * @param Config $config
	 *
	 * @return void
	 */
	protected function upload(string $path, $contents, Config $config): void
	{
		$path = $this->prefixer->prefixPath($path);

		$options = $this->getOptionsFromConfig($config);
		$options['name'] = $path;

		$this->bucket->upload($contents, $options);
	}

	/**
	 * @param string $path
	 * @throws UnableToReadFile
	 * @throws FilesystemException
	 * @return string
	 */
	public function read(string $path): string {
		try {
			return $this->getObject($path)->downloadAsString();
		} catch (Throwable $t) {
			throw UnableToReadFile::fromLocation($path, 'Error while reading from bucket: '.$t->getMessage(), $t);
		}
	}

	/**
	 * @param string $path
	 * @throws UnableToReadFile
	 * @throws FilesystemException
	 * @return resource
	 */
	public function readStream(string $path) {
		try {
			return StreamWrapper::getResource($this->getObject($path)->downloadAsStream());
		} catch (Throwable $t) {
			throw UnableToReadFile::fromLocation($path, 'Error while reading from bucket: '.$t->getMessage(), $t);
		}
	}

	public function delete(string $path): void {
		$this->getObject($path)->delete();
	}

	/**
	 * @param string $path
	 * @throws UnableToDeleteDirectory
	 * @throws FilesystemException
	 */
	public function deleteDirectory(string $path): void {
		try {
			$path = $this->normaliseDirName($path);
			$files = (new DirectoryListing($this->listContents($path, true)))
				->filter(fn(StorageAttributes $attributes) => $attributes->isFile())
				->getIterator();

			foreach ($files as $file) {
				$this->delete($file->path());
			}
		} catch (Throwable $t) {
			throw UnableToDeleteDirectory::atLocation($path, $t->getMessage(), $t);
		}
	}

	/**
	 * @param string $path
	 * @param Config $config
	 * @throws UnableToCreateDirectory
	 * @throws FilesystemException
	 */
	public function createDirectory(string $path, Config $config): void {
		try {
			$this->upload($this->normaliseDirName($path), '', $config);
		} catch (Throwable $t) {
			throw new UnableToCreateDirectory('Unable to create a directory at '.$path.'. '.$t->getMessage(), 0, $t);
		}
	}

	/**
	 * Returns a normalised directory name from the given path.
	 *
	 * @param string $dirname
	 *
	 * @return string
	 */
	protected function normaliseDirName(string $dirname): string {
		return rtrim($dirname, '/') . '/';
	}

	/**
	 * @param string $path
	 * @param string $visibility
	 * @throws InvalidVisibilityProvided
	 * @throws FilesystemException
	 */
	public function setVisibility(string $path, string $visibility): void {
		try {
			$object = $this->getObject($path);

			if ($visibility === Visibility::PRIVATE) {
				$object->acl()->delete('allUsers');
			} elseif ($visibility === Visibility::PUBLIC) {
				$object->acl()->add('allUsers', Acl::ROLE_READER);
			}
		} catch (Throwable $t) {
			throw UnableToSetVisibility::atLocation($path, $t->getMessage(), $t);
		}
	}

	/**
	 * @param string $path
	 * @throws UnableToRetrieveMetadata
	 * @throws FilesystemException
	 * @return FileAttributes
	 */
	public function visibility(string $path): FileAttributes {
		return $this->getFileAttributes($path, FileAttributes::ATTRIBUTE_VISIBILITY);
	}

	/**
	 * @param string $path
	 * @throws UnableToRetrieveMetadata
	 * @throws FilesystemException
	 * @return FileAttributes
	 */
	public function mimeType(string $path): FileAttributes {
		return $this->getFileAttributes($path, FileAttributes::ATTRIBUTE_MIME_TYPE);
	}

	/**
	 * @param string $path
	 * @throws UnableToRetrieveMetadata
	 * @throws FilesystemException
	 * @return FileAttributes
	 */
	public function lastModified(string $path): FileAttributes {
		return $this->getFileAttributes($path, FileAttributes::ATTRIBUTE_LAST_MODIFIED);
	}

	/**
	 * @param string $path
	 * @throws UnableToRetrieveMetadata
	 * @throws FilesystemException
	 * @return FileAttributes
	 */
	public function fileSize(string $path): FileAttributes {
		return $this->getFileAttributes($path, FileAttributes::ATTRIBUTE_FILE_SIZE);
	}

	/**
	 * @param string $path
	 * @param bool $deep
	 * @return iterable<StorageAttributes>
	 */
	public function listContents(string $path, bool $deep): iterable {
		$path = $this->prefixer->prefixPath($path);
		$objects = $this->bucket->objects(['prefix' => $path, 'delimiter' => DIRECTORY_SEPARATOR]);

		foreach ($objects->prefixes() as $directory) {
			yield new DirectoryAttributes($directory);
		}

		/** @var StorageObject $file */
		foreach ($objects as $file) {
			$updated = $this->getUpdated($file);
			yield new FileAttributes(
				$file->name(),
				(int)$file->info()['size'],
				$this->getRawVisibility($file),
				$updated,
				$file->info()['contentType'],
				$file->info()
			);
		}
	}

	/**
	 * @param string $source
	 * @param string $destination
	 * @param Config $config
	 * @throws UnableToMoveFile
	 * @throws FilesystemException
	 */
	public function move(string $source, string $destination, Config $config): void {
		try {
			$this->copy($source, $destination, $config);
			$this->delete($source);
		} catch (Throwable $t) {
			throw UnableToMoveFile::fromLocationTo($source, $destination, $t);
		}
	}

	/**
	 * @param string $source
	 * @param string $destination
	 * @param Config $config
	 * @throws UnableToCopyFile
	 * @throws FilesystemException
	 */
	public function copy(string $source, string $destination, Config $config): void {
		try {
			$destination = $this->prefixer->prefixPath($destination);

			// The new file shall have the same visibility as the original file
			$object = $this->getObject($source);
			$acl = $this->getPredefinedAclForVisibility($this->getRawVisibility($object));

			$object->copy($this->bucket, ['name' => $destination, 'predefinedAcl' => $acl]);
		} catch (Throwable $t) {
			throw UnableToCopyFile::fromLocationTo($source, $destination, $t);
		}
	}

	/**
	 * Returns a storage object for the given path.
	 *
	 * @param string $path
	 *
	 * @return StorageObject
	 */
	protected function getObject(string $path): StorageObject {
		$path = $this->prefixer->prefixPath($path);
		return $this->bucket->object($path);
	}

	/**
	 * Returns an array of options from the config.
	 *
	 * @param Config $config
	 *
	 * @return array
	 */
	protected function getOptionsFromConfig(Config $config): array {
		$options = [];

		if ($visibility = $config->get('visibility')) {
			$options['predefinedAcl'] = $this->getPredefinedAclForVisibility($visibility);
		} else {
			// If a file is created without an acl, it isn't accessible via the console.
			// Hence default to private
			$options['predefinedAcl'] = $this->getPredefinedAclForVisibility(Visibility::PRIVATE);
		}

		if ($metadata = $config->get('metadata')) {
			$options['metadata'] = $metadata;
		}

		return $options;
	}

	/**
	 * @param string $visibility
	 *
	 * @return string
	 */
	protected function getPredefinedAclForVisibility(string $visibility): string {
		return $visibility === Visibility::PUBLIC ? PredefinedAcl::PUBLIC_READ : PredefinedAcl::PROJECT_PRIVATE;
	}

	protected function getRawVisibility(StorageObject $object): string {
		try {
			$acl = $object->acl()->get(['entity' => 'allUsers']);
			return $acl['role'] === Acl::ROLE_READER ?
				Visibility::PUBLIC :
				Visibility::PRIVATE;
		} catch (NotFoundException $e) {
			// object may not have an acl entry, so handle that gracefully
			return Visibility::PRIVATE;
		}
	}

	protected function getUpdated(StorageObject $file): ?int {
		$updated = null;
		try {
			$updated = (new DateTime($file->info()['updated']))->getTimestamp();
		} catch (Exception $ex) {}
		return $updated;
	}

	/**
	 * @param string $path
	 * @param string $type
	 * @throws UnableToRetrieveMetadata
	 * @return FileAttributes
	 */
	protected function getFileAttributes(string $path, string $type): FileAttributes {
		try {
			$object = $this->getObject($path);
			return new FileAttributes(
				$path,
				$object->info()['size'],
				$this->getRawVisibility($object),
				$this->getUpdated($object),
				$object->info()['contentType'],
				$object->info()
			);
		} catch (Throwable $t) {
			throw UnableToRetrieveMetadata::create($path, $type, $t->getMessage(), $t);
		}
	}
}