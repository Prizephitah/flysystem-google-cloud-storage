<?php


namespace Superbalist\Flysystem\GoogleStorage;


final class PredefinedAcl {

	/**
	 * Project team owners get OWNER access, and allAuthenticatedUsers get READER access.
	 */
	public const AUTHENTICATED_READ = 'authenticatedRead';

	/**
	 * Project team owners get OWNER access.
	 */
	public const PRIVATE = 'private';

	/**
	 * Project team members get access according to their roles.
	 */
	public const PROJECT_PRIVATE = 'projectPrivate';

	/**
	 * Project team owners get OWNER access, and allUsers get READER access.
	 */
	public const PUBLIC_READ = 'publicRead';

	/**
	 * Project team owners get OWNER access, and allUsers get WRITER access.
	 */
	public const PUBLIC_READ_WRITE = 'publicReadWrite';
}