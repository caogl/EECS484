create table Album(
	album_id			varchar2(100),
	album_name			varchar2(100),
	album_cover_photo_id		varchar2(100) unique not null,
	album_created_time		timestamp(6),
	album_modified_time		timestamp(6),
	album_link			varchar2(2000),
	album_visibility		varchar2(100),
	primary key (album_id),
	constraint visibility_enumeration check(
		(album_visibility='EVERYONE') OR
		(album_visibility='FRIENDS') OR
		(album_visibility='MYSELF') OR
		(album_visibility='FRIENDS_OF_FRIENDS') OR
		(album_visibility='CUSTOM')
	)
);

create table Photo_In_Album(
	photo_id			varchar2(100),
	photo_caption			varchar2(2000),
	photo_created_time		timestamp(6),
	photo_modified_time		timestamp(6),
	photo_link			varchar2(2000),
	album_id			varchar2(100) not null,
	primary key (photo_id),
	foreign key (album_id) references Album(album_id)
);

alter table Album add constraint album_refs_cover_photo
	foreign key (album_cover_photo_id) references Photo_In_Album(photo_id)
		initially deferred deferrable;


create table Town (
	town_id				varchar(100),
	city				varchar(100),
	st				varchar(100),
	country				varchar(100),
	constraint unique_tuple unique(city, st, country),
	primary key (town_id)
);


create sequence town_id_sequence
	start with 1
	increment by 1;

create or replace trigger town_id_trigger before insert on Town
	for each row
	begin
	    select town_id_sequence.nextval
	    into :new.town_id
	    from dual;
	end;
.
run;

create table Users (
	user_id				varchar2(100),
	user_first_name			varchar2(100),
	user_last_name			varchar2(100),
	user_year_of_birth		number(38),
	user_month_of_birth		number(38),
	user_day_of_birth		number(38),
	user_gender			varchar2(100),
	hometown_id			varchar(100),
	primary key (user_id),
	foreign key (hometown_id) references Town(town_id)
);

create table Tagged_In (
	tag_x_coordinate		number,
	tag_y_coordinate		number,
	tag_created_time		timestamp(6),
	tag_photo_id			varchar2(100),
	tag_subject_id			varchar2(100),
	primary key (tag_photo_id, tag_subject_id),
	foreign key (tag_photo_id) references Photo_In_Album(photo_id),
	foreign key (tag_subject_id) references Users(user_id)
);

create table Owned (
	album_id			varchar2(100),
	user_id				varchar2(100),
	primary key (album_id, user_id),
	foreign key (album_id) references Album(album_id),
	foreign key (user_id) references Users(user_id)
);

create table Current_Location (
	user_id				varchar2(100),
	town_id				varchar2(100),
	location_number			number,
	primary key (user_id, town_id),
	foreign key (user_id) references Users(user_id),
	foreign key (town_id) references Town(town_id)
);

create table Event_Created(
	event_id			varchar2(100),
	event_name			varchar2(100),
	event_tagline			varchar2(1000),
	event_descr			varchar2(4000),
	event_host			varchar2(100),
	event_type			varchar2(100),
	event_subtype			varchar2(100),
	event_location			varchar2(200),
	event_start_time		timestamp(6),
	event_end_time			timestamp(6),
	user_id				varchar2(100) not null,
	primary key (event_id),
	foreign key (user_id) references Users(user_id)
);

create table Event_At(
	event_id			varchar2(100),
	town_id				varchar2(100),
	primary key (event_id),
	foreign key (event_id) references Event_Created,
	foreign key (town_id) references Town
);

create table Educated_At_School(
	educated_id			varchar2(100),
	year_of_graduation		number(38),
	concentration			varchar2(100),
	degree				varchar2(100),
	user_id				varchar2(100),
	school_name			varchar2(100),
	primary key (educated_id),
	foreign key (user_id) references Users(user_id)
);

create sequence educated_id_sequence
	start with 1
	increment by 1;

create or replace trigger educated_id_trigger before insert on Educated_At_School
	for each row
	begin
	    select educated_id_sequence.nextval
	    into :new.educated_id
	    from dual;
	end;
.
run;

create table Friend (
	user_id1			varchar(100),
	user_id2			varchar(100),
	primary key (user_id1, user_id2),
	check (user_id1 != user_id2),
	foreign key (user_id1) references Users,
	foreign key (user_id2) references Users
);

