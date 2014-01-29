


set autocommit off;
insert into Album(album_id, album_name, album_cover_photo_id,
		  album_created_time, album_modified_time, album_link, album_visibility)
	select distinct album_id, album_name, cover_photo_id,
		  album_created_time, album_modified_time, album_link, album_visibility
	from unnamed.public_photo_information;


insert into Photo_In_Album(photo_id, photo_caption, photo_created_time,
			   photo_modified_time, photo_link, album_id)
	select distinct photo_id, photo_caption, photo_created_time,
			   photo_modified_time, photo_link, album_id
	from unnamed.public_photo_information;

commit;
set autocommit on;



insert into Town(city, st, country)
	select distinct hometown_city, hometown_state, hometown_country
	from unnamed.public_user_information
		union
	select distinct current_city, current_state, current_country
	from unnamed.public_user_information
		union
	select distinct event_city, event_state, event_country
	from unnamed.public_event_information;




insert into Users(user_id, user_first_name, user_last_name, user_year_of_birth,
		  user_month_of_birth, user_day_of_birth, user_gender, hometown_id)
	select distinct
		unnamed.public_user_information.user_id,
		unnamed.public_user_information.first_name,
		unnamed.public_user_information.last_name,
		unnamed.public_user_information.year_of_birth,
		unnamed.public_user_information.month_of_birth,
		unnamed.public_user_information.day_of_birth,
		unnamed.public_user_information.gender,
		Town.town_id
	from unnamed.public_user_information
		inner join Town on
			unnamed.public_user_information.hometown_city=Town.city and
			unnamed.public_user_information.hometown_state=Town.st and
			unnamed.public_user_information.hometown_country=Town.country;


insert into Tagged_In(tag_x_coordinate,	tag_y_coordinate, tag_created_time, tag_photo_id, tag_subject_id)
	select distinct tag_x_coordinate, tag_y_coordinate, tag_created_time, photo_id, tag_subject_id
	from unnamed.public_tag_information;


insert into Owned(album_id, user_id)
	select distinct album_id, owner_id
	from unnamed.public_photo_information;


		
insert into Current_Location(user_id, town_id)
	select distinct 
		unnamed.public_user_information.user_id,
		Town.town_id
	from unnamed.public_user_information
		inner join Town on
			unnamed.public_user_information.current_city = Town.city and
			unnamed.public_user_information.current_state = Town.st and
			unnamed.public_user_information.current_country = Town.country;


insert into Event_Created(event_id, event_name, event_tagline, event_descr, event_host, event_type,
			  event_subtype, event_location, event_start_time, event_end_time, user_id)
	select distinct
		event_id, event_name, event_tagline, event_description, event_host, event_type,
		event_subtype, event_location, event_start_time, event_end_time, event_creator_id
	from unnamed.public_event_information;


insert into Event_At(event_id, town_id)
	select distinct
		unnamed.public_event_information.event_id,
		Town.town_id
	from unnamed.public_event_information
		inner join Town on
			unnamed.public_event_information.event_city = Town.city and
			unnamed.public_event_information.event_state = Town.st and
			unnamed.public_event_information.event_country = Town.country;


insert into Educated_At_School(year_of_graduation, concentration, degree, user_id, school_name)
	select distinct program_year, program_concentration, program_degree, user_id, institution_name
	from unnamed.public_user_information;
		


insert into Friend(user_id1, user_id2)
	select distinct user1_id, user2_id
	from unnamed.public_are_friends;








