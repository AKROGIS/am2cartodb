-- Quality Control Queries for CartoDB export of KATM_BrownBear project

-- Compare carto count to database counts
select count(*) as inCarto from Locations_In_CartoDB
select count(*) as inproject from Locations where ProjectId = 'KATM_BrownBear'
select count(*) as hidden from Locations where ProjectId = 'KATM_BrownBear' and status IS not NULL
select status, count(*) as outside from Locations as l join ProjectExportBoundaries as b on b.Project = l.ProjectId where ProjectId = 'KATM_BrownBear' and b.shape is not null and b.shape.STContains(l.Location) = 0 group by status
select count(*) as inCarto from Movements_In_CartoDB
select count(*) as inproject from Movements  where ProjectId = 'KATM_BrownBear'
select count(*) as degenerate from Movements  where ProjectId = 'KATM_BrownBear' and distance <= 0 
select count(*) as outside from Movements as m join ProjectExportBoundaries as b on b.Project = m.ProjectId where ProjectId = 'KATM_BrownBear' and distance > 0 and b.shape is not null and b.shape.STContains(m.Shape) = 0 


-- Locations to remove from Carto
select c.fixid from Locations_In_CartoDB as c
left join Locations as l on l.FixId = c.fixid
left join ProjectExportBoundaries as b on b.Project = l.ProjectId 
where l.FixId is null  -- not in location table any longer
or l.status is not null -- location is now hidden
or (b.shape is not null and b.shape.STContains(l.Location) = 0)  -- location is now outside boundary


-- Locations to add to Carto
select l.projectid, l.animalid, l.fixid, l.fixdate, location.Lat, Location.Long from locations as l
left join ProjectExportBoundaries as b on b.Project = l.ProjectId 
left join Locations_In_CartoDB as c on l.fixid = c.fixid
where c.FixId is null  -- not in CartoDB
and l.ProjectID = 'KATM_BrownBear' -- belongs to project
and l.[status] IS NULL -- not hidden
and (b.shape is null or b.Shape.STContains(l.Location) = 1)  -- inside boundary



-- Movements to remove from Carto (because they are not in the movements table any longer or not fullly inside the boundary)
select c.Projectid, c.AnimalId, c.StartDate, c.EndDate
from Movements_In_CartoDB as c left join movements as m
on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId
and m.StartDate = c.StartDate and m.EndDate = c.EndDate
left join ProjectExportBoundaries as b on b.Project = m.ProjectId
where m.projectid is null  -- not in movement database anylonger
or (b.shape is not null and b.shape.STContains(m.shape) = 0)  -- location is now outside boundary


-- Movements to add to Carto with spatial check
select m.Projectid, m.AnimalId, m.StartDate, m.EndDate, m.Duration, m.Distance, m.Speed, m.Shape.ToString() from movements as m
inner join ProjectExportBoundaries as b on b.Project = m.ProjectId
left join Movements_In_CartoDB as c on m.ProjectId = c.ProjectId and m.AnimalId = c.AnimalId and m.StartDate = c.StartDate and m.EndDate = c.EndDate
where c.ProjectId IS NULL  -- not in CartoDB
and m.ProjectId = 'KATM_BrownBear'  -- belongs to project
and Distance > 0 -- not a degenerate
and (b.shape is null or b.Shape.STContains(m.shape) = 1)  -- inside boundary



-- Locations not in Carto 
select * from Locations as l left join Locations_In_CartoDB as c on l.FixId = c.fixid where c.FixId is null and l.ProjectID = 'KATM_BrownBear' and l.status IS NULL

--for checking our carto count against online
select l.AnimalId, count(*) from Locations_In_CartoDB as c join Locations as l on l.FixId = c.fixid where l.ProjectID = 'KATM_BrownBear' and l.status IS NULL group by l.AnimalId
select animalid, count(*) from Movements_In_CartoDB group by animalid
